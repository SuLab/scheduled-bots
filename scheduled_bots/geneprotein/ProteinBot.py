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
import traceback
from datetime import datetime

import pymongo
from pymongo import MongoClient
from scheduled_bots.geneprotein import HelperBot
from scheduled_bots.geneprotein import organisms_info
from scheduled_bots.geneprotein.HelperBot import make_ref_source
from tqdm import tqdm
from wikidataintegrator import wdi_login, wdi_core, wdi_helpers
from wikidataintegrator.wdi_helpers import id_mapper

try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

PROPS = {'found in taxon': 'P703',
         'subclass of': 'P279',
         'encoded by': 'P702',
         'RefSeq Protein ID': 'P637',
         'UniProt ID': 'P352',
         'Ensembl Protein ID': 'P705',
         'OMIM ID': 'P492',
         'Entrez Gene ID': 'P351',

         'Saccharomyces Genome Database ID': 'P3406',
         'Mouse Genome Informatics ID': 'P671',
         }

__metadata__ = {'name': 'GeneBot',
                'maintainer': 'GSS',
                'tags': ['gene'],
                'properties': list(PROPS.values())
                }

# If the source is "entrez", the reference identifier to be used is "Ensembl Gene ID" (P594)
source_ref_id = {'ensembl': "Ensembl Gene ID",
                 'entrez': 'Entrez Gene ID',
                 'uniprot': None}


def create_item(record, organism_info, gene_wdid, login, write=True):
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

    external_ids = get_external_ids(record)
    statements = protein_item_statements(record, external_ids, organism_info['wdid'], gene_wdid, login=login)

    ############
    # names and labels
    ############

    item_name = record['name']['@value']
    if 'locus_tag' in record:
        item_name += " " + record['locus_tag']['@value']

    if record['taxid']['@value'] == 9606:
        item_description = 'protein of the species Homo sapiens'
    else:
        item_description = '{} protein found in {}'.format(organism_info['type'], organism_info['name'])

    aliases = [record['symbol']['@value']]

    try:
        wd_item_gene = wdi_core.WDItemEngine(item_name=item_name, domain='proteins', data=statements,
                                             append_value=[PROPS['subclass of']],
                                             fast_run=True,
                                             fast_run_base_filter={PROPS['UniProt ID']: '',
                                                                   PROPS['found in taxon']: organism_info['wdid']})
        wd_item_gene.set_label(item_name)
        wd_item_gene.set_description(item_description, lang='en')
        wd_item_gene.set_aliases(aliases)
        wdi_helpers.try_write(wd_item_gene, external_ids['UniProt ID'], PROPS['UniProt ID'], login, write=write)
    except Exception as e:
        exc_info = sys.exc_info()
        traceback.print_exception(*exc_info)
        msg = wdi_helpers.format_msg(external_ids['UniProt ID'], PROPS['UniProt ID'], None,
                                     str(e), msg_type=type(e))
        wdi_core.WDItemEngine.log("ERROR", msg)


def get_external_ids(record):
    ############
    # required external IDs
    ############

    uniprot_id = record['uniprot']['@value']['Swiss-Prot']
    entrez_gene = str(record['entrezgene']['@value'])

    external_ids = {'UniProt ID': uniprot_id, 'Entrez Gene ID': entrez_gene}

    ############
    # optional external IDs
    ############

    # SGD on both gene and protein item
    if 'SGD' in record:
        external_ids['Saccharomyces Genome Database ID'] = record['SGD']['@value']

    if 'MIM' in record:
        external_ids['OMIM ID'] = record['MIM']['@value']

    ############
    # optional external IDs (can have more than one)
    ############
    if 'protein' in record['ensembl']['@value']:
        # Ensembl Protein ID
        external_ids['Ensembl Protein ID'] = record['ensembl']['@value']['protein']
        if not isinstance(external_ids['Ensembl Protein ID'], list):
            external_ids['Ensembl Protein ID'] = [external_ids['Ensembl Protein ID']]

    if 'protein' in record['refseq']['@value']:
        # RefSeq Protein ID
        external_ids['RefSeq Protein ID'] = record['refseq']['@value']['protein']
        if not isinstance(external_ids['RefSeq Protein ID'], list):
            external_ids['RefSeq Protein ID'] = [external_ids['RefSeq Protein ID']]

    return external_ids


def protein_item_statements(record, external_ids, organism_wdid, gene_wdid, login=None):
    """
    construct list of referenced statements to pass to PBB_Core Item engine
    login is required to make reference items
    """
    s = []

    ############
    # ID statements
    # Required: uniprot (1)
    # Optional: OMIM (1?), Ensembl protein (0 or more), refseq protein (0 or more)
    ############
    uniprot_ref = make_ref_source(record['uniprot']['@source'], PROPS['UniProt ID'], external_ids['UniProt ID'],
                                  login=login)
    entrez_ref = make_ref_source(record['entrezgene']['@source'], PROPS['Entrez Gene ID'],
                                 external_ids['Entrez Gene ID'], login=login)


    s.append(wdi_core.WDString(external_ids['UniProt ID'], PROPS['UniProt ID'], references=[uniprot_ref]))

    for key in ['OMIM ID', 'Saccharomyces Genome Database ID']:
        if key in external_ids:
            s.append(wdi_core.WDString(external_ids[key], PROPS[key], references=[entrez_ref]))

    key = 'Ensembl Protein ID'
    if key in external_ids:
        for id in external_ids[key]:
            ref = make_ref_source(record['ensembl']['@source'], PROPS[key], id, login=login)
            s.append(wdi_core.WDString(id, PROPS[key], references=[ref]))

    key = 'RefSeq Protein ID'
    if key in external_ids:
        for id in external_ids[key]:
            ref = make_ref_source(record['refseq']['@source'], PROPS[key], id, login=login)
            s.append(wdi_core.WDString(id, PROPS[key], references=[ref]))

    ############
    # Protein statements
    ############
    # subclass of protein
    # TODO: I actually have no idea where this comes from (reference)
    s.append(wdi_core.WDItemID("Q8054", PROPS['subclass of'], references=[entrez_ref]))

    # found in taxon
    # TODO: I actually have no idea where this comes from (reference)
    s.append(wdi_core.WDItemID(organism_wdid, PROPS['found in taxon'], references=[entrez_ref]))

    # encoded by
    s.append(wdi_core.WDItemID(gene_wdid, PROPS['encoded by'], references=[entrez_ref]))

    return s


def main(coll: pymongo.collection.Collection, taxid: str, retrieved: datetime,
         log_dir: str = "./logs", write: bool = True) -> None:
    """
    Main function for creating/updating genes

    :param coll: mongo collection containing gene data from mygene
    :param taxid: taxon to use (ncbi tax id)
    :param retrieved:
    :param log_dir: dir to store logs
    :param write: actually perform write
    :return:
    """

    # make sure the organism is found in wikidata
    taxid = int(taxid)
    organism_wdid = wdi_helpers.prop2qid("P685", taxid)
    if not organism_wdid:
        raise ValueError("organism {} not found".format(taxid))

    # login
    login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS)
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(__metadata__))

    if taxid not in organisms_info:
        raise ValueError("organism_info not found")
    organism_info = organisms_info[taxid]

    # get all entrez gene id -> wdid mappings, where found in taxon is this strain
    gene_wdid_mapping = id_mapper("P351", (("P703", organism_info['wdid']),))

    # only do certain records
    docs = coll.find({'taxid': taxid, 'type_of_gene': 'protein-coding'})
    docs = HelperBot.validate_docs(docs, PROPS['Entrez Gene ID'])
    records = HelperBot.tag_mygene_docs(docs)

    for record in tqdm(records, mininterval=2):
        gene_wdid = gene_wdid_mapping[str(record['entrezgene']['@value'])]
        create_item(record, organism_info, gene_wdid, login, write=write)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='run wikidata GO bot')
    parser.add_argument('--log-dir', help='directory to store logs', type=str)
    parser.add_argument('--dummy', help='do not actually do write', action='store_true')
    parser.add_argument('--taxon', help="only run using this taxon (ncbi tax id)", type=str)
    parser.add_argument('--mongo-uri', type=str, default="mongodb://localhost:27017")
    parser.add_argument('--mongo-db', type=str, default="wikidata_src")
    parser.add_argument('--mongo-coll', type=str, default="mygene")
    parser.add_argument('--retrieved', help="date go annotations were retrieved (YYYYMMDD)", type=str)
    args = parser.parse_args()
    log_dir = args.log_dir if args.log_dir else "./logs"
    run_id = datetime.now().strftime('%Y%m%d_%H:%M')
    taxon = args.taxon
    coll = MongoClient(args.mongo_uri)[args.mongo_db][args.mongo_coll]
    retrieved = datetime.strptime(args.retrieved, "%Y%m%d") if args.retrieved else datetime.now()

    log_name = '{}-{}.log'.format(__metadata__['name'], datetime.now().strftime('%Y%m%d_%H:%M'))
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(__metadata__),
                                        logger_name='go{}'.format(taxon))

    main(coll, taxon, retrieved, log_dir=log_dir, write=not args.dummy)
