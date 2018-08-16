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

from scheduled_bots import PROPS
from scheduled_bots.geneprotein.Downloader import MyGeneDownloader, LocalDownloader
from wikidataintegrator import wdi_login, wdi_core, wdi_helpers
from wikidataintegrator.wdi_helpers import wait_for_last_modified

from scheduled_bots.geneprotein.bots.GeneBot import ChromosomalGeneBot, MicrobeGeneBot, HumanGeneBot
from scheduled_bots.geneprotein.bots.ProteinBot import ProteinBot

from scheduled_bots.geneprotein import HelperBot, organisms_info
from scheduled_bots.geneprotein.ChromosomeBot import ChromosomeBot
from scheduled_bots.geneprotein.MicrobialChromosomeBot import MicrobialChromosomeBot
from scheduled_bots.geneprotein.HelperBot import parse_mygene_src_version, source_items
from scheduled_bots.geneprotein.helpers.StatementFactory import StatementFactory, ChromosomalStatementFactory, HumanStatementFactory

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

def main(taxid, metadata, log_dir="./logs", run_id=None, fast_run=True, write=True, entrez=None, filepath=None,
         locus_tag=None, refseq=False, deprecated_entrez=False, chromosome=None, gene=False, protein=False, no_chromosome=False):
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
            gene_bot = HumanGeneBot(organism_info, login, HumanStatementFactory(chr_num_wdid))
        else:
            gene_bot = ChromosomalGeneBot(organism_info, chr_num_wdid, login, ChromosomalStatementFactory(chr_num_wdid))
    else:
        # check if its one of the reference microbial genomes
        # raises valueerror if not...
        mcb = MicrobialChromosomeBot()
        organism_info = mcb.get_organism_info(taxid)
        if not no_chromosome:
            if chromosome:
                refseq_qid_chrom = {chromosome.split(":")[0]: chromosome.split(":")[1]}
            else:
                refseq_qid_chrom = mcb.get_or_create_chromosomes(taxid, login)
            factory = ChromosomalStatementFactory(refseq_qid_chrom)
        else:
            factory = StatementFactory()
        print(organism_info)
        gene_bot = MicrobeGeneBot(organism_info, login, factory)
        validate_type = "microbial"

    # Get handle to mygene records
    if filepath:
        downloader = LocalDownloader(path=filepath)
    else:
        downloader = MyGeneDownloader()

    if (gene):
        if entrez:
            doc, total = downloader.get_mg_gene(entrez)
            docs = iter([doc])
        elif locus_tag:
            doc, total = downloader.get_mg_gene(locustag=locus_tag)
            docs = iter([doc])
        else:
            docs, total = downloader.get_mg_cursor(taxid, downloader.get_filter())

        print("total number of gene records: {}".format(total))
        # the scroll_id/cursor times out from mygene if we iterate. So.... get the whole thing now
        docs = list(docs)
        docs = HelperBot.validate_docs(docs, validate_type, PROPS['Entrez Gene ID'])
        records = HelperBot.tag_mygene_docs(docs, metadata, downloader.get_key_source())
        gene_bot.run(records, total=total, fast_run=fast_run, write=write, refseq=refseq, deprecated_entrez=deprecated_entrez)
        if protein:
            wait_for_last_modified(datetime.now())

    if (protein):
        protein_bot = ProteinBot(organism_info, login, no_chromosome=no_chromosome)
        if entrez:
            doc, total = downloader.get_mg_gene(entrez)
            docs = iter([doc])
        elif locus_tag:
            doc, total = downloader.get_mg_gene(locustag=locus_tag)
            docs = iter([doc])
        else:
            docs, total = downloader.get_mg_cursor(taxid, downloader.get_filter(protein=True))
        print("total number of protein-coding gene records: {}".format(total))
        # the scroll_id/cursor times out from mygene if we iterate. So.... get the whole thing now
        docs = list(docs)
        docs = HelperBot.validate_docs(docs, validate_type, PROPS['Entrez Gene ID'])
        records = HelperBot.tag_mygene_docs(docs, metadata, downloader.get_key_source())
        protein_bot.run(records, total=total, write=write, refseq=refseq, fast_run=fast_run)

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

    if gene:
        gene_bot.cleanup(releases_to_remove, last_updated)
    if protein:
        protein_bot.cleanup(releases_to_remove, last_updated)

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
    parser.add_argument('--chromosome', help="use specific chromosome in form 'refseq:qid' ex. 'NZ_AOBT01000001.1:Q56085906'", type=str)
    parser.set_defaults(fastrun=True)
    parser.add_argument('--gene', help='whether or not to run the gene bot', action='store_true')
    parser.add_argument('--protein', help='whether or not to run the protein bot', action='store_true')
    parser.add_argument('--no-chromosome', dest='no_chromosome', help='whether or not to run the chromosome bot', action='store_true')
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
             write=not args.dummy, entrez=args.entrez, filepath=args.filepath, refseq=args.refseq,
             deprecated_entrez=args.deprecated_entrez, chromosome=args.chromosome, gene=args.gene, protein=args.protein, no_chromosome=args.no_chromosome)
        sys.exit(0)

    if args.locus_tag:
        main(taxon, metadata, run_id=run_id, log_dir=log_dir, fast_run=fast_run,
             write=not args.dummy, locus_tag=args.locus_tag, filepath=args.filepath, refseq=args.refseq,
             deprecated_entrez=args.deprecated_entrez, chromosome=args.chromosome, gene=args.gene, protein=args.protein, no_chromosome=args.no_chromosome)
        sys.exit(0)

    if args.microbes:
        mcb = MicrobialChromosomeBot()
        microbe_taxa = mcb.get_all_taxids()
        taxon = taxon + ','.join(map(str, microbe_taxa))

    for taxon1 in taxon.split(","):
        try:
            main(taxon1, metadata, run_id=run_id, log_dir=log_dir, fast_run=fast_run, write=not args.dummy,
                 filepath=args.filepath, refseq=args.refseq, deprecated_entrez=args.deprecated_entrez,
                 chromosome=args.chromosome, gene=args.gene, protein=args.protein, no_chromosome=args.no_chromosome)
        except Exception as e:
            # if one taxon fails, still try to run the others
            traceback.print_exc()
        # done with this run, clear fast run container to save on RAM
        wdi_core.WDItemEngine.fast_run_store = []
        wdi_core.WDItemEngine.fast_run_container = None
