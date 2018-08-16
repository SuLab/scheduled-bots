from scheduled_bots.geneprotein import type_of_gene_map, descriptions_by_type
from itertools import chain
from wikidataintegrator import wdi_core
from wikidataintegrator.ref_handlers import update_retrieved_if_new
from scheduled_bots import get_default_core_props, PROPS

core_props = get_default_core_props()


class Gene:
    """
    Generic gene class. Subclasses: Human, Mammal, Microbe
    """

    def __init__(self, record, organism_info, ref_factory, stat_factory):
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
        self.ref_factory = ref_factory
        self.stat_factory = stat_factory

        self.wd_item_gene = None
        self.label = None
        self.description = None
        self.aliases = None
        self.external_ids = dict()
        self.type_of_gene = None
        self.statements = None
        self.entrez = None
        self.entrez_ref = None
        self.ensembl_ref = None

    def create_description(self):
        if self.type_of_gene is None:
            raise ValueError("must set type_of_gene first")
        self.description = descriptions_by_type[self.type_of_gene].format(self.organism_info['name'])

    def create_label(self):
        self.label = self.record['name']['@value']

    def create_aliases(self):
        if self.label is None:
            self.create_label()
        aliases = []
        if 'symbol' in self.record and self.record['symbol']['@value'] != '':
            aliases.append(self.record['symbol']['@value'])
        if 'name' in self.record:
            aliases.append(self.record['name']['@value'])
        if 'NCBI Locus tag' in self.external_ids:
            aliases.append(self.external_ids['NCBI Locus tag'])
        if 'alias' in self.record:
            aliases.extend(self.record['alias']['@value'])
        aliases = set(aliases) - {self.label} - set(descriptions_by_type.keys())
        self.aliases = list(aliases)

    def set_label_desc_aliases(self, wditem):
        wditem.set_label(self.label)
        curr_descr = wditem.get_description()
        if curr_descr == "" or "of the species" in curr_descr or "gene found in" in curr_descr.lower():
            wditem.set_description(self.description)
        wditem.set_aliases(self.aliases)
        return wditem

    def validate_record(self):
        # handled by HelperBot
        # allow for subclasses to add additional checks
        raise NotImplementedError()

    def parse_external_ids(self):
        ############
        # required external IDs
        ############

        entrez_gene = str(self.record['entrezgene']['@value'])
        self.external_ids['Entrez Gene ID'] = entrez_gene
        self.entrez = entrez_gene
        taxid = self.record['taxid']['@value']

        ############
        # optional external IDs
        ############
        # taxid, example gene
        # mouse: 10090, 102466888
        # rat: 10116, 100362233
        # sgd: 559292, 853415
        # fly: 7227, 31303
        # worm: 6239, 174065
        # zfin: 7955, 368434

        # ncbi locus tag
        if 'locus_tag' in self.record:
            self.external_ids['NCBI Locus tag'] = self.record['locus_tag']['@value']

        if 'MGI' in self.record:
            self.external_ids['Mouse Genome Informatics ID'] = self.record['MGI']['@value']
        if 'RGD' in self.record:
            self.external_ids['Rat Genome Database ID'] = self.record['RGD']['@value']
        if 'SGD' in self.record:
            self.external_ids['Saccharomyces Genome Database ID'] = self.record['SGD']['@value']
        if 'FLYBASE' in self.record:
            self.external_ids['FlyBase Gene ID'] = self.record['FLYBASE']['@value']
        if 'WormBase' in self.record:
            self.external_ids['Wormbase Gene ID'] = self.record['WormBase']['@value']
        if 'ZFIN' in self.record:
            self.external_ids['ZFIN Gene ID'] = self.record['ZFIN']['@value']

        if 'HGNC' in self.record:
            self.external_ids['HGNC ID'] = self.record['HGNC']['@value']

        if taxid == 9606 and 'symbol' in self.record and 'HGNC' in self.record:
            # see: https://github.com/stuppie/scheduled-bots/issues/2
            # "and 'HGNC' in record" is required because there is something wrong with mygene
            self.external_ids['HGNC Gene Symbol'] = self.record['symbol']['@value']

        if taxid == 10090 and 'symbol' in self.record:
            self.external_ids['MGI Gene Symbol'] = self.record['symbol']['@value']

        if 'homologene' in self.record:
            self.external_ids['HomoloGene ID'] = str(self.record['homologene']['@value']['id'])

        if 'map_location' in self.record:
            self.external_ids['cytogenetic location'] = self.record['map_location']['@value']

        ############
        # optional external IDs (can have more than one)
        ############
        if 'ensembl' in self.record:
            ensembl_transcript = set(chain(*[x['transcript'] for x in self.record['ensembl']['@value']]))
            self.external_ids['Ensembl Transcript ID'] = ensembl_transcript
            ensembl_gene = [x['gene'] for x in self.record['ensembl']['@value']]
            self.external_ids['Ensembl Gene ID'] = ensembl_gene

        # RefSeq RNA ID
        if 'refseq' in self.record and 'rna' in self.record['refseq']['@value']:
            self.external_ids['RefSeq RNA ID'] = self.record['refseq']['@value']['rna']

        if 'refseq' in self.record and 'genomic' in self.record['refseq']['@value']:
            self.external_ids['RefSeq Genome ID'] = self.record['refseq']['@value']['genomic']

    def create_ref_sources(self):
        # create an entrez ref and ensembl ref (optional)
        if 'Entrez Gene ID' in self.external_ids and self.external_ids['Entrez Gene ID']:
            self.entrez_ref = self.ref_factory.get_reference(self.record['entrezgene']['@source'],
                                                             PROPS['Entrez Gene ID'],
                                                             self.external_ids['Entrez Gene ID'])
        if 'Ensembl Gene ID' in self.external_ids and self.external_ids['Ensembl Gene ID']:
            if len(self.external_ids['Ensembl Gene ID']) != 1:
                raise ValueError("more than one ensembl gene ID: {}".format(self.record['entrezgene']))
            ensembl_gene_id = list(self.external_ids['Ensembl Gene ID'])[0]
            self.ensembl_ref = self.ref_factory.get_reference(self.record['ensembl']['@source'],
                                                              PROPS['Ensembl Gene ID'],
                                                              ensembl_gene_id)

        if 'RefSeq Genome ID' in self.external_ids and self.external_ids['RefSeq Genome ID']:
            self.refseq_ref = self.ref_factory.get_reference(self.record['refseq']['@source'],
                                                             PROPS['Refseq Genome ID'],
                                                             self.external_ids['RefSeq Genome ID'])

    def create_statements(self, refseq=False, deprecated_entrez=False):
        return self.stat_factory.create_statements(self.record, self.organism_info, self.external_ids, self.entrez_ref,
                                                   self.ensembl_ref, self.refseq_ref, refseq=refseq,
                                                   deprecated_entrez=deprecated_entrez)

    def create_item(self, fast_run=True, refseq=False, deprecated_entrez=False):
        '''
        create_item will return the wd_item of the gene from wikidata (if it exists) with its new data appended.
        Does not write to wikidata
        :param fast_run: whether or not to query the gene in fast run mode
        :return: the wd_item of the gene with its new data
        '''
        self.parse_external_ids()
        self.create_ref_sources()
        self.statements = self.create_statements(refseq=refseq, deprecated_entrez=deprecated_entrez)
        # remove subclass of gene statements
        # s = wdi_core.WDItemID("Q7187", "P279")
        # setattr(s, 'remove', '')
        self.create_label()
        self.create_description()
        self.create_aliases()

        self.fast_run_base_filter = {PROPS['Entrez Gene ID']: '',
                                     PROPS['found in taxon']: self.organism_info['wdid']}

        self.wd_item_gene = wdi_core.WDItemEngine(item_name=self.label, domain='genes', data=self.statements,
                                                  append_value=[PROPS['instance of']],
                                                  fast_run=fast_run, fast_run_base_filter=self.fast_run_base_filter,
                                                  fast_run_use_refs=True, ref_handler=update_retrieved_if_new,
                                                  global_ref_mode="CUSTOM",
                                                  core_props=core_props)

        self.wd_item_gene = self.set_label_desc_aliases(self.wd_item_gene)

        return self.wd_item_gene

    def get_id_and_prop(self):
        return self.external_ids['Entrez Gene ID'], PROPS['Entrez Gene ID']
