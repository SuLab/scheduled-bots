"""
example human gene
https://www.wikidata.org/wiki/Q14911732
https://mygene.info/v3/gene/1017
https://www.ncbi.nlm.nih.gov/gene/1017
http://uswest.ensembl.org/Homo_sapiens/Gene/Summary?g=ENSG00000123374;r=12:55966769-55972784

example mouse gene
https://www.wikidata.org/wiki/Q21129787

example yeast gene:
https://www.wikidata.org/wiki/Q27539933
https://mygene.info/v3/gene/856615

example microbial gene:
https://www.wikidata.org/wiki/Q23097138
https://mygene.info/v3/gene/7150837

sparql query for listing current subclasses: http://tinyurl.com/y8ecgka7

"""
# TODO: Gene on two chromosomes
# https://www.wikidata.org/wiki/Q20787772

import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime
from functools import partial

from tqdm import tqdm
from gene.ReferenceFactory import ReferenceFactory

from scheduled_bots import get_default_core_props, PROPS
from scheduled_bots.geneprotein.Downloader import MyGeneDownloader, LocalDownloader
from wikidataintegrator import wdi_login, wdi_core, wdi_helpers
from wikidataintegrator.ref_handlers import update_retrieved_if_new
from wikidataintegrator.wdi_fastrun import FastRunContainer

from gene.HumanGene import HumanGene
from gene.MicrobeGene import MicrobeGene
from gene.ChromosomalGene import ChromosomalGene

core_props = get_default_core_props()

FASTRUN_PROPS = {'Entrez Gene ID', 'strand orientation', 'Saccharomyces Genome Database ID', 'RefSeq RNA ID',
                 'ZFIN Gene ID', 'Ensembl Transcript ID', 'HGNC ID', 'encodes', 'genomic assembly', 'found in taxon',
                 'HomoloGene ID', 'MGI Gene Symbol', 'cytogenetic location', 'Mouse Genome Informatics ID',
                 'FlyBase Gene ID', 'genomic end', 'NCBI Locus tag', 'Rat Genome Database ID', 'Ensembl Gene ID',
                 'instance of', 'chromosome', 'HGNC Gene Symbol', 'Wormbase Gene ID', 'genomic start'}

from scheduled_bots.geneprotein import HelperBot, organisms_info, type_of_gene_map, descriptions_by_type
from scheduled_bots.geneprotein.ChromosomeBot import ChromosomeBot
from scheduled_bots.geneprotein.MicrobialChromosomeBot import MicrobialChromosomeBot
from scheduled_bots.geneprotein.HelperBot import make_ref_source, parse_mygene_src_version, source_items
from gene.DeprecatedStatements import remove_deprecated_statements

try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

__metadata__ = {
    'name': 'GeneBot',
    'maintainer': 'GSS',
    'tags': ['gene'],
}

class GeneBot:
    """
    Generic genebot class
    """
    item = None
    failed = []  # list of entrez ids for those that failed

    def __init__(self, organism_info, login):
        self.login = login
        self.organism_info = organism_info

    def run(self, records, total=None, fast_run=True, write=True):
        # this shouldn't ever actually get used now
        raise NotImplementedError("The gene bot has not implemented a run method!")
        '''records = self.filter(records)
        for record in tqdm(records, mininterval=2, total=total):
            gene = self.GENE_CLASS(type, record, self.organism_info, self.login)
            try:
                gene.create_item(fast_run=fast_run, write=write)
            except Exception as e:
                exc_info = sys.exc_info()
                traceback.print_exception(*exc_info)
                msg = wdi_helpers.format_msg(gene.external_ids['Entrez Gene ID'], PROPS['Entrez Gene ID'], None,
                                             str(e), msg_type=type(e))
                wdi_core.WDItemEngine.log("ERROR", msg)
                gene.status = msg

            if gene.status is not True:
                self.failed.append(gene.entrez)'''

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
        """

        :param releases:
        :param last_updated:
        :param failed: list of entrez ids to skip
        :return:
        """
        print(self.failed)
        entrez_qid = wdi_helpers.id_mapper('P351', ((PROPS['found in taxon'], self.organism_info['wdid']),))
        print(len(entrez_qid))
        entrez_qid = {entrez: qid for entrez, qid in entrez_qid.items() if entrez not in self.failed}
        print(len(entrez_qid))
        filter = {PROPS['Entrez Gene ID']: '', PROPS['found in taxon']: self.organism_info['wdid']}
        frc = FastRunContainer(wdi_core.WDBaseDataType, wdi_core.WDItemEngine, base_filter=filter, use_refs=True)
        frc.clear()
        props = [PROPS[x] for x in FASTRUN_PROPS]
        for qid in tqdm(entrez_qid.values()):
            remove_deprecated_statements(qid, frc, releases, last_updated, props, self.login)


class ChromosomalGeneBot(GeneBot):
    GENE_CLASS = ChromosomalGene

    def __init__(self, organism_info, chr_num_wdid, login):
        super().__init__(organism_info, login)
        self.chr_num_wdid = chr_num_wdid

    def run(self, records, total=None, fast_run=True, write=True, refseq=False, deprecated_entrez=False):
        records = self.filter(records)
        for record in tqdm(records, mininterval=2, total=total):
            gene = self.GENE_CLASS(record, self.organism_info, self.chr_num_wdid, ReferenceFactory(self.login))
            try:
                item = gene.create_item(fast_run=fast_run, refseq=refseq, deprecated_entrez=deprecated_entrez)
                id, prop = gene.get_id_and_prop()
                status = wdi_helpers.try_write(item, id, prop, self.login, write=write)
            except Exception as e:
                exc_info = sys.exc_info()
                traceback.print_exception(*exc_info)
                id, prop = gene.get_id_and_prop()
                msg = wdi_helpers.format_msg(id, prop, None, str(e), msg_type=type(e))
                wdi_core.WDItemEngine.log("ERROR", msg)
                status = msg
            if status is not True:
                self.failed.append(gene.entrez)

class HumanGeneBot(ChromosomalGeneBot):
    GENE_CLASS = HumanGene

class MicrobeGeneBot(ChromosomalGeneBot):
    GENE_CLASS = MicrobeGene

def main(taxid, metadata, log_dir="./logs", run_id=None, fast_run=True, write=True, entrez=None, filepath=None,
         locus_tag=None, refseq=False, deprecated_entrez=False):
    """
    Main function for creating/updating genes

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
    :param entrez: Only run this one gene
    :type entrez: int
    :param locus_tag: Only run this one gene
    :type locus_tag: str
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
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, logger_name='WD_logger', log_name=log_name,
                                        header=json.dumps(__metadata__))

    # get organism metadata (name, organism type, wdid)
    # TODO: this can be pulled from wd
    if taxid in organisms_info and organisms_info[taxid]['type'] != "microbial":
        validate_type = 'eukaryotic'
        organism_info = organisms_info[taxid]
        # make sure all chromosome items are found in wikidata
        cb = ChromosomeBot()
        chr_num_wdid = cb.get_or_create(organism_info, login=login)
        chr_num_wdid = {k.upper(): v for k, v in chr_num_wdid.items()}
        if int(organism_info['taxid']) == 9606:
            bot = HumanGeneBot(organism_info, chr_num_wdid, login)
        else:
            bot = ChromosomalGeneBot(organism_info, chr_num_wdid, login)
    else:
        # check if its one of the reference microbial genomes
        # raises valueerror if not...
        mcb = MicrobialChromosomeBot()
        organism_info = mcb.get_organism_info(taxid)
        refseq_qid_chrom = mcb.get_or_create_chromosomes(taxid, login)
        print(organism_info)
        bot = MicrobeGeneBot(organism_info, refseq_qid_chrom, login)
        validate_type = "microbial"

    # Get handle to mygene records
    if filepath:
        downloader = LocalDownloader(path=filepath)
    else:
        downloader = MyGeneDownloader()

    if entrez:
        doc, total = downloader.get_mg_gene(entrez)
        docs = iter([doc])
    elif locus_tag:
        doc, total = downloader.get_mg_gene(locustag=locus_tag)
        docs = iter([doc])
    else:
        docs, total = downloader.get_mg_cursor(taxid, downloader.get_filter())

    print("total number of records: {}".format(total))
    # the scroll_id/cursor times out from mygene if we iterate. So.... get the whole thing now
    docs = list(docs)
    docs = HelperBot.validate_docs(docs, validate_type, PROPS['Entrez Gene ID'])
    records = HelperBot.tag_mygene_docs(docs, metadata, downloader.get_key_source())

    bot.run(records, total=total, fast_run=fast_run, write=write, refseq=refseq, deprecated_entrez=deprecated_entrez)
    for frc in wdi_core.WDItemEngine.fast_run_store:
        frc.clear()

    if write:
        print("done updating, waiting 10 min")
        time.sleep(10 * 60)

    releases = dict()
    releases_to_remove = set()
    last_updated = dict()
    metadata = {k: v for k, v in metadata.items() if k in {'uniprot', 'ensembl', 'entrez', 'refseq'}}
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

if __name__ == "__main__":
    """
    Data to be used is retrieved from mygene.info or from a local json file in a similar format
    For local file:
        Specify with --filepath './some/path/data.json'
        file requires a JSON list with 3 elements: [docs, metadata, key_source]
            docs - contains a list of docs in same format as mygene.info,
            metadata - contains metadata in same format as mygene.info endpoint,
            key_source - contains mapping of keys in a doc to their source
    """
    parser = argparse.ArgumentParser(description='run wikidata gene bot')
    parser.add_argument('--log-dir', help='directory to store logs', type=str)
    parser.add_argument('--dummy', help='do not actually do write', action='store_true')
    parser.add_argument('--taxon',
                        help="only run using this taxon (ncbi tax id). comma separated",
                        type=str, required=True)
    parser.add_argument('--all-microbes', dest='microbes', help="run using all microbe taxids", action='store_true')
    parser.add_argument('--fastrun', dest='fastrun', action='store_true')
    parser.add_argument('--no-fastrun', dest='fastrun', action='store_false')
    parser.add_argument('--entrez', help="Run only this one gene with specified entrez")
    parser.add_argument('--locus_tag', help="Run only this one gene with specified locus_tag")
    parser.add_argument('--filepath', help='load gene information from a local file instead of mygene.info', type=str)
    parser.add_argument('--refseq', dest='refseq', help='use refseq references over entrez', action='store_true')
    parser.add_argument('--deprecated_entrez', dest='deprecated_entrez', help='whether or not the entrez id, if present, is deprecated', action='store_true')
    parser.set_defaults(fastrun=True)
    args = parser.parse_args()
    log_dir = args.log_dir if args.log_dir else "./logs"
    run_id = datetime.now().strftime('%Y%m%d_%H:%M')
    __metadata__['run_id'] = run_id
    taxon = args.taxon
    fast_run = args.fastrun

    # get metadata about sources
    if args.filepath:
        downloader = LocalDownloader(path=args.filepath)
    else:
        downloader = MyGeneDownloader()

    metadata = downloader.get_metadata()['src_version']

    if args.entrez:
        main(taxon, metadata, run_id=run_id, log_dir=log_dir, fast_run=fast_run,
             write=not args.dummy, entrez=args.entrez, filepath=args.filepath, refseq=args.refseq, deprecated_entrez=args.deprecated_entrez)
        sys.exit(0)

    if args.locus_tag:
        main(taxon, metadata, run_id=run_id, log_dir=log_dir, fast_run=fast_run,
             write=not args.dummy, locus_tag=args.locus_tag, filepath=args.filepath, refseq=args.refseq, deprecated_entrez=args.deprecated_entrez)
        sys.exit(0)

    if args.microbes:
        mcb = MicrobialChromosomeBot()
        microbe_taxa = mcb.get_all_taxids()
        taxon = taxon + ','.join(map(str, microbe_taxa))

    for taxon1 in taxon.split(","):
        try:
            main(taxon1, metadata, run_id=run_id, log_dir=log_dir, fast_run=fast_run, write=not args.dummy,
                 filepath=args.filepath, refseq=args.refseq, deprecated_entrez=args.deprecated_entrez)
        except Exception as e:
            # if one taxon fails, still try to run the others
            traceback.print_exc()
        # done with this run, clear fast run container to save on RAM
        wdi_core.WDItemEngine.fast_run_store = []
        wdi_core.WDItemEngine.fast_run_container = None
