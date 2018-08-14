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
