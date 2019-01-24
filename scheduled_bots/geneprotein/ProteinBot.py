"""

example human protein
https://www.wikidata.org/wiki/Q511968
https://mygene.info/v3/gene/1017
https://www.ncbi.nlm.nih.gov/gene/1017
http://uswest.ensembl.org/Homo_sapiens/Gene/Summary?g=ENSG00000123374;r=12:55966769-55972784

example mouse protein
https://www.wikidata.org/wiki/Q14911733

example yeast protein:
https://www.wikidata.org/wiki/Q27547285
https://mygene.info/v3/gene/856615

example microbial protein:
https://www.wikidata.org/wiki/Q22161590
https://mygene.info/v3/gene/7150837

"""

import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime
from itertools import chain

from tqdm import tqdm

from scheduled_bots import get_default_core_props, PROPS
from scheduled_bots.geneprotein import HelperBot, descriptions_by_type
from scheduled_bots.geneprotein import organisms_info
from scheduled_bots.geneprotein.Downloader import MyGeneDownloader
from scheduled_bots.geneprotein.GeneBot import remove_deprecated_statements
from scheduled_bots.geneprotein.HelperBot import make_ref_source, parse_mygene_src_version, source_items
from scheduled_bots.geneprotein.MicrobeBotResources import get_all_taxa, get_organism_info
from wikidataintegrator import wdi_login, wdi_core, wdi_helpers
from wikidataintegrator.ref_handlers import update_retrieved_if_new
from wikidataintegrator.wdi_fastrun import FastRunContainer
from wikidataintegrator.wdi_helpers import id_mapper, format_msg

core_props = get_default_core_props()

FASTRUN_PROPS = {'Entrez Gene ID', 'encodes', 'OMIM ID', 'Ensembl Protein ID', 'encoded by', 'instance of',
                 'found in taxon', 'Mouse Genome Informatics ID', 'Saccharomyces Genome Database ID',
                 'RefSeq Protein ID', 'UniProt ID'}


try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

__metadata__ = {
    'name': 'ProteinBot',
    'maintainer': 'GSS',
    'tags': ['protein'],
}

# If the source is "entrez", the reference identifier to be used is "Ensembl Gene ID" (P594)
source_ref_id = {
    'ensembl': "Ensembl Protein ID",
    'entrez': 'Entrez Gene ID',
    'uniprot': 'UniProt ID'
}


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
        if 'locus_tag' in self.record:
            self.label += " " + self.record['locus_tag']['@value']
        self.label = self.label[0].upper() + self.label[1:]

    def create_aliases(self):
        aliases = [self.record['symbol']['@value']]
        if 'NCBI Locus tag' in self.external_ids:
            aliases.append(self.external_ids['NCBI Locus tag'])
        if 'other_names' in self.record:
            aliases.extend(self.record['other_names']['@value'])
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

    def create_statements(self):
        """
        create statements common to all proteins
        """
        s = []

        ############
        # ID statements
        # Required: uniprot (1)
        # Optional: OMIM (1?), Ensembl protein (0 or more), refseq protein (0 or more)
        ############
        entrez_gene = self.external_ids['Entrez Gene ID']
        uniprot_ref = make_ref_source(self.record['uniprot']['@source'], PROPS['UniProt ID'],
                                      self.external_ids['UniProt ID'],
                                      login=self.login)
        entrez_ref = make_ref_source(self.record['entrezgene']['@source'], PROPS['Entrez Gene ID'],
                                     self.external_ids['Entrez Gene ID'], login=self.login)

        s.append(wdi_core.WDString(self.external_ids['UniProt ID'], PROPS['UniProt ID'], references=[uniprot_ref]))

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
                ref = make_ref_source(self.record['refseq']['@source'], PROPS['Entrez Gene ID'], entrez_gene,
                                      login=self.login)
                s.append(wdi_core.WDString(id, PROPS[key], references=[ref]))

        ############
        # Protein statements
        ############
        # instance of protein
        s.append(wdi_core.WDItemID("Q8054", PROPS['instance of'], references=[uniprot_ref]))

        # found in taxon
        s.append(wdi_core.WDItemID(self.organism_info['wdid'], PROPS['found in taxon'], references=[uniprot_ref]))

        # encoded by
        s.append(wdi_core.WDItemID(self.gene_wdid, PROPS['encoded by'], references=[uniprot_ref]))

        return s

    def make_gene_encodes(self, write=True):
        """
        Add an "encodes" statement to the gene item
        :return:
        """
        uniprot_ref = make_ref_source(self.record['uniprot']['@source'], PROPS['UniProt ID'],
                                      self.external_ids['UniProt ID'],
                                      login=self.login)

        try:
            statements = [wdi_core.WDItemID(self.protein_wdid, PROPS['encodes'], references=[uniprot_ref])]
            wd_item_gene = wdi_core.WDItemEngine(wd_item_id=self.gene_wdid,data=statements,
                                                 append_value=[PROPS['encodes']], fast_run=fast_run,
                                                 fast_run_base_filter={PROPS['Entrez Gene ID']: '',
                                                                       PROPS['found in taxon']: self.organism_info[
                                                                           'wdid']},
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

    def create_item(self, fast_run=True, write=True):
        try:
            self.parse_external_ids()
            self.statements = self.create_statements()
            self.create_label()
            self.create_description()
            self.create_aliases()

            wd_item_protein = wdi_core.WDItemEngine(data=self.statements,
                                                    append_value=[PROPS['instance of'], PROPS['encoded by']],
                                                    # PROPS['Ensembl Protein ID'], PROPS['RefSeq Protein ID']],
                                                    fast_run=fast_run,
                                                    fast_run_base_filter={PROPS['UniProt ID']: '',
                                                                          PROPS['found in taxon']: self.organism_info[
                                                                              'wdid']},
                                                    fast_run_use_refs=True, ref_handler=update_retrieved_if_new,
                                                    global_ref_mode="CUSTOM",
                                                    core_props=core_props)
            wd_item_protein.set_label(self.label)
            wd_item_protein.set_description(self.description, lang='en')

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

    def update_item(self, qid, fast_run=True, write=True):
        print("updating protein: {}".format(qid))
        try:
            self.parse_external_ids()
            self.statements = self.create_statements()

            wd_item_protein = wdi_core.WDItemEngine(wd_item_id=qid, data=self.statements,
                                                    append_value=[PROPS['instance of'], PROPS['encoded by'],
                                                                  PROPS['Ensembl Protein ID'],
                                                                  PROPS['RefSeq Protein ID']],
                                                    fast_run=fast_run,
                                                    fast_run_base_filter={PROPS['UniProt ID']: '',
                                                                          PROPS['found in taxon']: self.organism_info[
                                                                              'wdid']},
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


class ProteinBot:
    """
    Generic proteinbot class
    """
    failed = []  # list of uniprot ids for those that failed

    def __init__(self, organism_info, gene_wdid_mapping, login):
        self.login = login
        self.organism_info = organism_info
        self.gene_wdid_mapping = gene_wdid_mapping
        self.uniprot_qid = dict()

    def run(self, records, total=None, fast_run=True, write=True):
        records = self.filter(records)
        for record in tqdm(records, mininterval=2, total=total):
            entrez_gene = str(record['entrezgene']['@value'])
            if entrez_gene not in self.gene_wdid_mapping:
                wdi_core.WDItemEngine.log("WARNING", format_msg(entrez_gene, "P351", None,
                                                                "Gene item not found during protein creation", None))
                continue
            gene_wdid = self.gene_wdid_mapping[entrez_gene]

            # handle multiple protiens
            if 'uniprot' in record and 'Swiss-Prot' in record['uniprot']['@value']:
                uniprots = record['uniprot']['@value']['Swiss-Prot']
                for uniprot in uniprots:
                    record['uniprot']['@value']['Swiss-Prot'] = uniprot
                    self.run_one(record, gene_wdid, write)
            else:
                self.run_one(record, gene_wdid, write)

    def run_one(self, record, gene_wdid, write):
        protein = Protein(record, self.organism_info, gene_wdid, self.login)
        try:
            protein.parse_external_ids()
            uniprot = protein.external_ids['UniProt ID']
        except Exception as e:
            msg = wdi_helpers.format_msg(gene_wdid, None, None, str(e), msg_type=type(e))
            wdi_core.WDItemEngine.log("ERROR", msg)
            return

        # some proteins are encoded by multiple genes. don't try to create it again
        if uniprot in self.uniprot_qid:
            qid = self.uniprot_qid[uniprot]
            wditem = protein.update_item(qid, fast_run=fast_run, write=write)
        else:
            wditem = protein.create_item(fast_run=fast_run, write=write)
        if wditem is not None:
            self.uniprot_qid[uniprot] = wditem.wd_item_id
            protein.make_gene_encodes(write=write)
        if protein.status is not True:
            self.failed.append(protein.uniprot)

    def filter(self, records):
        """
        This is used to selectively skip certain records based on conditions within the record or to specifically
        alter certain fields before sending to the Bot
        """
        # If we are processing zebrafish records, skip the record if it doesn't have a zfin ID
        for record in records:
            if record['taxid']['@value'] == 7955 and 'ZFIN' not in record:
                continue
            else:
                yield record

    def cleanup(self, releases, last_updated):
        print(self.failed)
        uniprot_wdid = wdi_helpers.id_mapper(PROPS['UniProt ID'],
                                             ((PROPS['found in taxon'], self.organism_info['wdid']),))
        print(len(uniprot_wdid))
        uniprot_wdid = {uniprot: qid for uniprot, qid in uniprot_wdid.items() if uniprot not in self.failed}
        print(len(uniprot_wdid))
        filter = {PROPS['UniProt ID']: '', PROPS['found in taxon']: self.organism_info['wdid']}
        frc = FastRunContainer(wdi_core.WDBaseDataType, wdi_core.WDItemEngine, base_filter=filter, use_refs=True)
        frc.clear()
        props = [PROPS[x] for x in FASTRUN_PROPS]
        for qid in tqdm(uniprot_wdid.values()):
            remove_deprecated_statements(qid, frc, releases, last_updated, props, self.login)


def main(taxid, metadata, log_dir="./logs", run_id=None, fast_run=True, write=True, entrez=None):
    """
    Main function for creating/updating proteins

    :param taxid: taxon to use (ncbi tax id)
    :type taxid: str
    :param metadata: looks like: {"ensembl" : 84, "cpdb" : 31, "netaffy" : "na35", "ucsc" : "20160620", .. }
    :type metadata: dict
    :param log_dir: dir to store logs
    :type log_dir: str
    :param fast_run: use fast run mode
    :type fast_run: bool
    :param write: actually perform write
    :type write: bool
    :param entrez: Only run this one protein (given by entrezgene id)
    :type entrez: int
    :return: None
    """

    # make sure the organism is found in wikidata
    taxid = int(taxid)
    organism_wdid = wdi_helpers.prop2qid("P685", str(taxid))
    if not organism_wdid:
        print("organism {} not found in wikidata".format(taxid))
        return None

    # login
    login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS)
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []
        wdi_core.WDItemEngine.logger.handlers = []

    run_id = run_id if run_id is not None else datetime.now().strftime('%Y%m%d_%H:%M')
    log_name = '{}-{}.log'.format(__metadata__['name'], run_id)
    __metadata__['taxid'] = taxid
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(__metadata__))

    # get organism metadata (name, organism type, wdid)
    if taxid in organisms_info:
        validate_type = 'eukaryotic'
        organism_info = organisms_info[taxid]
    else:
        # check if its one of the microbe refs
        # raises valueerror if not...
        organism_info = get_organism_info(taxid)
        validate_type = 'microbial'
        print(organism_info)

    # get all entrez gene id -> wdid mappings, where found in taxon is this strain
    gene_wdid_mapping = id_mapper("P351", (("P703", organism_info['wdid']),))

    bot = ProteinBot(organism_info, gene_wdid_mapping, login)

    # Get handle to mygene records
    mgd = MyGeneDownloader()
    if entrez:
        doc, total = mgd.get_mg_gene(entrez)
        docs = iter([doc])
    else:
        doc_filter = lambda x: (x.get("type_of_gene") == "protein-coding") and ("uniprot" in x) and ("entrezgene" in x)
        docs, total = mgd.get_mg_cursor(taxid, doc_filter)
    print("total number of records: {}".format(total))
    # the scroll_id/cursor times out from mygene if we iterate. So.... get the whole thing now
    docs = list(docs)
    docs = HelperBot.validate_docs(docs, validate_type, PROPS['Entrez Gene ID'])
    records = HelperBot.tag_mygene_docs(docs, metadata)

    bot.run(records, total=total, fast_run=fast_run, write=write)
    for frc in wdi_core.WDItemEngine.fast_run_store:
        frc.clear()

    time.sleep(10 * 60)
    releases = dict()
    releases_to_remove = set()
    last_updated = dict()
    metadata = {k: v for k, v in metadata.items() if k in {'uniprot', 'ensembl', 'entrez'}}
    for k, v in parse_mygene_src_version(metadata).items():
        if "release" in v:
            if k not in releases:
                releases[k] = wdi_helpers.id_mapper('P393', (('P629', source_items[k]),))
            to_remove = set(releases[k].values())
            to_remove.discard(releases[k][v['release']])
            releases_to_remove.update(to_remove)
            print(
                "{}: Removing releases: {}, keeping release: {}".format(k, ", ".join(set(releases[k]) - {v['release']}),
                                                                        v['release']))
        else:
            last_updated[source_items[k]] = datetime.strptime(v["timestamp"], "%Y%m%d")
    print(last_updated)
    bot.cleanup(releases_to_remove, last_updated)

    # after the run is done, disconnect the logging handler
    # so that if we start another, it doesn't write twice
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []


if __name__ == "__main__":
    """
    Data to be used is retrieved from mygene.info
    """
    parser = argparse.ArgumentParser(description='run wikidata protein bot')
    parser.add_argument('--log-dir', help='directory to store logs', type=str)
    parser.add_argument('--dummy', help='do not actually do write', action='store_true')
    parser.add_argument('--taxon',
                        help="only run using this taxon (ncbi tax id). or 'microbe' for all microbes. comma separated",
                        type=str)
    parser.add_argument('--fastrun', dest='fastrun', action='store_true')
    parser.add_argument('--no-fastrun', dest='fastrun', action='store_false')
    parser.add_argument('--entrez', help="Run only this one protein (specified by entrez gene ID)")
    parser.set_defaults(fastrun=True)
    args = parser.parse_args()
    log_dir = args.log_dir if args.log_dir else "./logs"
    run_id = datetime.now().strftime('%Y%m%d_%H:%M')
    __metadata__['run_id'] = run_id
    taxon = args.taxon
    fast_run = args.fastrun

    # get metadata about sources
    mgd = MyGeneDownloader()
    metadata = mgd.get_metadata()['src_version']

    log_name = '{}-{}.log'.format(__metadata__['name'], run_id)
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(__metadata__),
                                        logger_name='protein{}'.format(taxon))

    if args.entrez:
        main(taxon, metadata, run_id=run_id, log_dir=log_dir, fast_run=fast_run,
             write=not args.dummy, entrez=args.entrez)
        sys.exit(0)

    if "microbe" in taxon:
        microbe_taxa = get_all_taxa()
        taxon = taxon.replace("microbe", ','.join(map(str, microbe_taxa)))

    for taxon1 in taxon.split(","):
        main(taxon1, metadata, log_dir=log_dir, fast_run=fast_run, write=not args.dummy)
        # done with this run, clear fast run container to save on RAM
        wdi_core.WDItemEngine.fast_run_store = []
        wdi_core.WDItemEngine.fast_run_container = None
