import sys
import traceback
from itertools import chain

from scheduled_bots import get_default_core_props, PROPS
from scheduled_bots.geneprotein import descriptions_by_type
from scheduled_bots.geneprotein.HelperBot import make_ref_source
from wikidataintegrator import wdi_core, wdi_helpers
from wikidataintegrator.ref_handlers import update_retrieved_if_new

core_props = get_default_core_props()


class Protein:
    """
    Generic protein class
    """
    record = None
    label = None
    description = None
    aliases = None
    external_ids = None
    status = None

    def __init__(self, record, organism_info, gene_wdid, login):
        """
        generate pbb_core item object

        :param record: dict from mygene,tagged with @value and @source
        :param organism_info: looks like {
            "type": "fungal",
            "name": "Saccharomyces cerevisiae S288c",
            "wdid": "Q27510868",
            'taxid': 559292
        }

        :param login:

        """
        self.record = record
        self.organism_info = organism_info
        self.gene_wdid = gene_wdid
        self.login = login

        self.statements = None
        self.protein_wdid = None
        self.uniprot = None

    def create_description(self):
        if self.organism_info['type']:
            self.description = '{} protein found in {}'.format(self.organism_info['type'], self.organism_info['name'])
        else:
            self.description = 'Protein found in {}'.format(self.organism_info['name'])

    def create_label(self):
        self.label = self.record['name']['@value']
        if 'locus_tag' in self.record and self.record['locus_tag']['@value'] not in self.label:
            self.label += " " + self.record['locus_tag']['@value']
        self.label = self.label[0].upper() + self.label[1:]

    def create_aliases(self):
        aliases = []
        if 'symbol' in self.record and self.record['symbol']['@value'] != '':
            aliases.append(self.record['symbol']['@value'])
        if 'locus_tag' in self.record:
            aliases.append(self.record['locus_tag']['@value'])
        if 'other_names' in self.record:
            aliases.append(self.record['other_names']['@value'])
        if 'alias' in self.record:
            aliases.append(self.record['alias']['@value'])
        aliases = set(aliases) - {self.label} - set(descriptions_by_type.keys())
        self.aliases = list(aliases)

    def parse_external_ids(self):
        ############
        # required external IDs
        # only using items with exactly one swiss-prot or trembl ID
        ############

        entrez_gene = str(self.record['entrezgene']['@value'])
        self.external_ids = {'Entrez Gene ID': entrez_gene}

        if 'Swiss-Prot' in self.record['uniprot']['@value']:
            uniprot_id = self.record['uniprot']['@value']['Swiss-Prot']
        elif 'TrEMBL' in self.record['uniprot']['@value'] and len(self.record['uniprot']['@value']['TrEMBL']) == 1:
            uniprot_id = self.record['uniprot']['@value']['TrEMBL'][0]
        else:
            raise ValueError("no uniprot found")

        self.external_ids['UniProt ID'] = uniprot_id
        self.uniprot = uniprot_id

        ############
        # optional external IDs
        ############
        # SGD on both gene and protein item
        if 'SGD' in self.record:
            self.external_ids['Saccharomyces Genome Database ID'] = self.record['SGD']['@value']

        ############
        # optional external IDs (can have more than one)
        ############
        if 'ensembl' in self.record:
            ensembl_protein = set(chain(*[x['protein'] for x in self.record['ensembl']['@value']]))
            self.external_ids['Ensembl Protein ID'] = ensembl_protein

        if 'refseq' in self.record and 'protein' in self.record['refseq']['@value']:
            # RefSeq Protein ID
            self.external_ids['RefSeq Protein ID'] = self.record['refseq']['@value']['protein']

        if 'refseq' in self.record and 'genomic' in self.record['refseq']['@value']:
            self.external_ids['RefSeq Genome ID'] = self.record['refseq']['@value']['genomic']

    def create_statements(self, refseq=False):
        """
        create statements common to all proteins
        """
        s = []

        ############
        # ID statements
        # Required: uniprot (1)
        # Optional: OMIM (1?), Ensembl protein (0 or more), refseq protein (0 or more)
        ############
        uniprot_ref = None
        if self.external_ids['UniProt ID']:
            uniprot_ref = make_ref_source(self.record['uniprot']['@source'], PROPS['UniProt ID'],
                                      self.external_ids['UniProt ID'],
                                      login=self.login)

        entrez_ref = None
        if 'Entrez Gene ID' in self.external_ids and self.external_ids['Entrez Gene ID']:
            entrez_ref = make_ref_source(self.record['entrezgene']['@source'], PROPS['Entrez Gene ID'],
                self.external_ids['Entrez Gene ID'], login=self.login)

        refseq_ref = None
        if 'RefSeq Genome ID' in self.external_ids and self.external_ids['RefSeq Genome ID']:
            refseq_ref = make_ref_source(self.record['refseq']['@source'], PROPS['Refseq Genome ID'],
                                                             self.external_ids['RefSeq Genome ID'], login=self.login)

        if uniprot_ref:
            s.append(wdi_core.WDString(self.external_ids['UniProt ID'], PROPS['UniProt ID'], references=[uniprot_ref]))

        if entrez_ref:
            for key in ['Saccharomyces Genome Database ID']:
                if key in self.external_ids:
                    s.append(wdi_core.WDString(self.external_ids[key], PROPS[key], references=[entrez_ref]))

        key = 'Ensembl Protein ID'
        if key in self.external_ids:
            for id in self.external_ids[key]:
                ref = make_ref_source(self.record['ensembl']['@source'], PROPS[key], id, login=self.login)
                s.append(wdi_core.WDString(id, PROPS[key], references=[ref]))

        key = 'RefSeq Protein ID'
        if key in self.external_ids:
            for id in self.external_ids[key]:
                ref = []
                if entrez_ref:
                    ref = entrez_ref
                elif refseq_ref:
                    ref = refseq_ref

                if refseq and refseq_ref:
                    ref = refseq_ref
                s.append(wdi_core.WDString(id, PROPS[key], references=[ref]))

        ############
        # Protein statements
        ############
        # instance of protein
        ref = None
        if refseq and refseq_ref:
            ref = refseq_ref
        elif uniprot_ref:
            ref = uniprot_ref
        s.append(wdi_core.WDItemID("Q8054", PROPS['instance of'], references=[ref]))

        # found in taxon
        s.append(wdi_core.WDItemID(self.organism_info['wdid'], PROPS['found in taxon'], references=[ref]))

        # encoded by
        s.append(wdi_core.WDItemID(self.gene_wdid, PROPS['encoded by'], references=[ref]))

        return s

    def make_gene_encodes(self, fast_run=True, write=True, refseq=False):
        """
        Add an "encodes" statement to the gene item
        :return:
        """
        if 'RefSeq Genome ID' in self.external_ids and self.external_ids['RefSeq Genome ID']:
            refseq_ref = make_ref_source(self.record['refseq']['@source'], PROPS['Refseq Genome ID'],
                                                             self.external_ids['RefSeq Genome ID'], login=self.login)

        if self.external_ids['UniProt ID']:
            uniprot_ref = make_ref_source(self.record['uniprot']['@source'], PROPS['UniProt ID'],
                                      self.external_ids['UniProt ID'],
                                      login=self.login)

        ref = None
        if refseq and refseq_ref:
            ref = refseq_ref
        elif uniprot_ref:
            ref = uniprot_ref

        try:
            statements = [wdi_core.WDItemID(self.protein_wdid, PROPS['encodes'], references=[ref])]
            wd_item_gene = wdi_core.WDItemEngine(wd_item_id=self.gene_wdid, domain='genes', data=statements,
                                                 append_value=[PROPS['encodes']], fast_run=fast_run,
                                                 fast_run_base_filter={PROPS['found in taxon']: self.organism_info['wdid']},
                                                 global_ref_mode="CUSTOM", ref_handler=update_retrieved_if_new,
                                                 core_props=core_props)
            wdi_helpers.try_write(wd_item_gene, self.external_ids['UniProt ID'], PROPS['UniProt ID'], self.login,
                                  write=write)
        except Exception as e:
            exc_info = sys.exc_info()
            traceback.print_exception(*exc_info)
            msg = wdi_helpers.format_msg(self.external_ids['UniProt ID'], PROPS['UniProt ID'], None,
                                         str(e), msg_type=type(e))
            wdi_core.WDItemEngine.log("ERROR", msg)

    def create_item(self, fast_run=True, write=True, refseq=False):
        try:
            self.parse_external_ids()
            self.statements = self.create_statements(refseq=refseq)
            self.create_label()
            self.create_description()
            self.create_aliases()

            wd_item_protein = wdi_core.WDItemEngine(item_name=self.label, domain='proteins', data=self.statements,
                                                    append_value=[PROPS['instance of'], PROPS['encoded by']],
                                                    # PROPS['Ensembl Protein ID'], PROPS['RefSeq Protein ID']],
                                                    fast_run=fast_run,
                                                    fast_run_base_filter={PROPS['found in taxon']: self.organism_info['wdid']},
                                                    fast_run_use_refs=True, ref_handler=update_retrieved_if_new,
                                                    global_ref_mode="CUSTOM",
                                                    core_props=core_props)

            wd_item_protein.set_label(self.label)
            wd_item_protein.set_description(self.description)

            # remove the alias "protein"
            current_aliases = set(wd_item_protein.get_aliases())
            aliases = current_aliases | set(self.aliases)
            if "protein" in aliases:
                aliases.remove("protein")

            wd_item_protein.set_aliases(aliases, append=False)

            self.status = wdi_helpers.try_write(wd_item_protein, self.external_ids['UniProt ID'], PROPS['UniProt ID'],
                                                self.login,
                                                write=write)
            self.protein_wdid = wd_item_protein.wd_item_id

            return wd_item_protein
        except Exception as e:
            exc_info = sys.exc_info()
            traceback.print_exception(*exc_info)
            msg = wdi_helpers.format_msg(self.external_ids['Entrez Gene ID'], PROPS['Entrez Gene ID'], None,
                                         str(e), msg_type=type(e))
            wdi_core.WDItemEngine.log("ERROR", msg)
            self.status = msg
            return None

    def update_item(self, qid, fast_run=True, write=True, refseq=False):
        print("updating protein: {}".format(qid))
        try:
            self.parse_external_ids()
            self.statements = self.create_statements(refseq=refseq)
            wd_item_protein = wdi_core.WDItemEngine(wd_item_id=qid, data=self.statements,
                                                    append_value=[PROPS['instance of'], PROPS['encoded by'],
                                                                  PROPS['Ensembl Protein ID'],
                                                                  PROPS['RefSeq Protein ID']],
                                                    fast_run=fast_run,
                                                    fast_run_base_filter={PROPS['found in taxon']: self.organism_info['wdid']},
                                                    fast_run_use_refs=True, ref_handler=update_retrieved_if_new,
                                                    global_ref_mode="CUSTOM",
                                                    core_props=core_props)
            wdi_helpers.try_write(wd_item_protein, self.external_ids['UniProt ID'], PROPS['UniProt ID'], self.login,
                                  write=write)
            self.protein_wdid = wd_item_protein.wd_item_id
            return wd_item_protein
        except Exception as e:
            exc_info = sys.exc_info()
            traceback.print_exception(*exc_info)
            msg = wdi_helpers.format_msg(self.external_ids['Entrez Gene ID'], PROPS['Entrez Gene ID'], None,
                                         str(e), msg_type=type(e))
            wdi_core.WDItemEngine.log("ERROR", msg)
            return None
