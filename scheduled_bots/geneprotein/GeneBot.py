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

"""
#TODO: Gene on two chromosomes
#https://www.wikidata.org/wiki/Q20787772


import argparse
import json
import os
import sys
import traceback
from datetime import datetime

import pymongo
from pymongo import MongoClient
from scheduled_bots.geneprotein import HelperBot
from scheduled_bots.geneprotein import type_of_gene_map, organisms_info
from scheduled_bots.geneprotein.ChromosomeBot import ChromosomeBot
from scheduled_bots.geneprotein.HelperBot import make_ref_source
from tqdm import tqdm
from wikidataintegrator import wdi_login, wdi_core, wdi_helpers

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
         'strand orientation': 'P2548',
         'Entrez Gene ID': 'P351',
         'NCBI Locus tag': 'P2393',
         'Ensembl Gene ID': 'P594',
         'Ensembl Transcript ID': 'P704',
         'genomic assembly': 'P659',
         'genomic start': 'P644',
         'genomic end': 'P645',
         'chromosome': 'P1057',
         'Saccharomyces Genome Database ID': 'P3406',
         'Mouse Genome Informatics ID': 'P671',
         'HGNC ID': 'P354',
         'HGNC Gene Symbol': 'P353',
         'RefSeq RNA ID': 'P639',
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


def create_item(record, organism_info, chr_num_wdid, login, write=True):
    """
    generate pbb_core item object

    :param record: dict from mygene,tagged with @value and @source
    :param organism_info: looks like {
        "type": "fungal",
        "name": "Saccharomyces cerevisiae S288c",
        "wdid": "Q27510868",
        'taxid': 559292
    }
    :param chr_num_wdid: mapping of chr number (str) to wdid
    :param login:

    """

    external_ids = get_external_ids(record)
    statements = gene_item_statements(record, external_ids, organism_info['wdid'], chr_num_wdid, login=login)

    ############
    # names and labels
    ############

    if 'symbol' in record:
        item_name = record['symbol']['@value']
    else:
        item_name = record['name']['@value']

    if record['taxid']['@value'] == 9606:
        item_description = 'gene of the species Homo sapiens'
    else:
        item_description = '{} gene found in {}'.format(organism_info['type'], organism_info['name'])

    aliases = [record['symbol']['@value']]
    if 'NCBI Locus tag' in external_ids:
        aliases.append(external_ids['NCBI Locus tag'])

    try:
        wd_item_gene = wdi_core.WDItemEngine(item_name=item_name, domain='genes', data=statements,
                                             append_value=[PROPS['subclass of']],
                                             fast_run=True,
                                             fast_run_base_filter={PROPS['Entrez Gene ID']: '',
                                                                   PROPS['found in taxon']: organism_info['wdid']})
        wd_item_gene.set_label(item_name)
        wd_item_gene.set_description(item_description, lang='en')
        wd_item_gene.set_aliases(aliases)
        wdi_helpers.try_write(wd_item_gene, external_ids['Entrez Gene ID'], PROPS['Entrez Gene ID'], login, write=write)
    except Exception as e:
        exc_info = sys.exc_info()
        traceback.print_exception(*exc_info)
        msg = wdi_helpers.format_msg(external_ids['Entrez Gene ID'], PROPS['Entrez Gene ID'], None,
                                     str(e), msg_type=type(e))
        wdi_core.WDItemEngine.log("ERROR", msg)


def get_external_ids(record):
    ############
    # required external IDs
    ############

    entrez_gene = str(record['entrezgene']['@value'])

    ensembl_gene = record['ensembl']['@value']['gene']

    external_ids = {'Entrez Gene ID': entrez_gene, 'Ensembl Gene ID': ensembl_gene}

    ############
    # optional external IDs
    ############
    if 'locus_tag' in record:
        # ncbi locus tag
        external_ids['NCBI Locus tag'] = record['locus_tag']['@value']

    if 'SGD' in record:
        external_ids['Saccharomyces Genome Database ID'] = record['SGD']['@value']

    if 'HGNC' in record:
        external_ids['HGNC ID'] = record['HGNC']['@value']

    if 'symbol' in record and 'HGNC' in record:
        # "and 'HGNC' in record" is required because there is something wrong with mygene
        # see: https://github.com/stuppie/scheduled-bots/issues/2
        external_ids['HGNC Gene Symbol'] = record['symbol']['@value']

    if 'MGI' in record:
        external_ids['Mouse Genome Informatics ID'] = record['MGI']['@value']

    ############
    # optional external IDs (can have more than one)
    ############
    if 'transcript' in record['ensembl']['@value']:
        # Ensembl Transcript ID
        external_ids['Ensembl Transcript ID'] = record['ensembl']['@value']['transcript']
        if not isinstance(external_ids['Ensembl Transcript ID'], list):
            external_ids['Ensembl Transcript ID'] = [external_ids['Ensembl Transcript ID']]

    if 'rna' in record['refseq']['@value']:
        # RefSeq RNA ID
        external_ids['RefSeq RNA ID'] = record['refseq']['@value']['rna']
        if not isinstance(external_ids['RefSeq RNA ID'], list):
            external_ids['RefSeq RNA ID'] = [external_ids['RefSeq RNA ID']]

    return external_ids


def gene_item_statements(record, external_ids, organism_wdid, chr_num_wdid, login=None):
    """
    construct list of referenced statements to pass to PBB_Core Item engine
    login is required to make reference items
    """
    s = []

    ############
    # ID statements
    ############
    ensembl_ref = make_ref_source(record['ensembl']['@source'], PROPS['Ensembl Gene ID'],
                                  external_ids['Ensembl Gene ID'], login=login)
    entrez_ref = make_ref_source(record['entrezgene']['@source'], PROPS['Entrez Gene ID'],
                                 external_ids['Entrez Gene ID'], login=login)

    s.append(wdi_core.WDString(external_ids['Entrez Gene ID'], PROPS['Entrez Gene ID'], references=[entrez_ref]))
    s.append(wdi_core.WDString(external_ids['Ensembl Gene ID'], PROPS['Ensembl Gene ID'], references=[ensembl_ref]))

    for key in ['NCBI Locus tag', 'HGNC ID', 'HGNC Gene Symbol']:
        if key in external_ids:
            s.append(wdi_core.WDString(external_ids[key], PROPS[key], references=[entrez_ref]))

    key = 'Mouse Genome Informatics ID'
    if key in external_ids:
        source_doc = record['MGI']['@source']
        s.append(wdi_core.WDString(external_ids[key], PROPS[key],
                                   references=[make_ref_source(source_doc, PROPS[key], external_ids[key])]))

    key = 'Saccharomyces Genome Database ID'
    if key in external_ids:
        source_doc = record['SGD']['@source']
        s.append(wdi_core.WDString(external_ids[key], PROPS[key],
                                   references=[make_ref_source(source_doc, PROPS[key], external_ids[key])]))

    key = 'Ensembl Transcript ID'
    if key in external_ids:
        for id in external_ids[key]:
            s.append(wdi_core.WDString(id, PROPS[key], references=[ensembl_ref]))

    key = 'RefSeq RNA ID'
    if key in external_ids:
        for id in external_ids[key]:
            s.append(wdi_core.WDString(id, PROPS[key], references=[entrez_ref]))

    ############
    # Gene statements
    ############
    # subclass of gene/protein-coding gene/etc
    # TODO: I actually have no idea where this comes from (reference)
    type_of_gene = record['type_of_gene']['@value']
    s.append(wdi_core.WDItemID(type_of_gene_map[type_of_gene], PROPS['subclass of'], references=[ensembl_ref]))

    # found in taxon
    # TODO: I actually have no idea where this comes from (reference)
    s.append(wdi_core.WDItemID(organism_wdid, PROPS['found in taxon'], references=[ensembl_ref]))

    ############
    # genomic position: start, end, strand orientation, chromosome
    ############
    human = True if organism_wdid == "Q15978631" else False

    # todo: fix chromosomes
    # example: http://mygene.info/v3/gene/2952?fields=genomic_pos,genomic_pos_hg19

    if human:
        gp_statements = do_gp_human(record, chr_num_wdid, external_ids, login=login)
    else:
        gp_statements = do_gp_non_human(record, chr_num_wdid, external_ids, login=login)
    s.extend(gp_statements)

    return s


def do_gp_human(record, chr_num_wdid, external_ids, login=None):
    genomic_pos_value = record['genomic_pos']['@value']
    genomic_pos_source = record['genomic_pos']['@source']
    genomic_pos_id_prop = source_ref_id[genomic_pos_source['id']]
    genomic_pos_ref = make_ref_source(genomic_pos_source, PROPS[genomic_pos_id_prop],
                                      external_ids[genomic_pos_id_prop], login=login)
    assembly = wdi_core.WDItemID("Q20966585", PROPS['genomic assembly'], is_qualifier=True)

    # create qualifier for start/stop
    chrom_wdid = chr_num_wdid[genomic_pos_value['chr']]
    qualifiers = [wdi_core.WDItemID(chrom_wdid, PROPS['chromosome'], is_qualifier=True), assembly]

    strand_orientation = 'Q22809680' if genomic_pos_value['strand'] == 1 else 'Q22809711'

    if 'genomic_pos_hg19' in record:
        do_hg19 = True
        genomic_pos_value_hg19 = record['genomic_pos_hg19']['@value']
        genomic_pos_source_hg19 = record['genomic_pos_hg19']['@source']
        genomic_pos_id_prop_hg19 = source_ref_id[genomic_pos_source_hg19['id']]
        genomic_pos_ref_hg19 = make_ref_source(genomic_pos_source_hg19, PROPS[genomic_pos_id_prop_hg19],
                                               external_ids[genomic_pos_id_prop_hg19], login=login)
        assembly_hg19 = wdi_core.WDItemID("Q21067546", PROPS['genomic assembly'], is_qualifier=True)
        chrom_wdid_hg19 = chr_num_wdid[genomic_pos_value_hg19['chr']]
        qualifiers_hg19 = [wdi_core.WDItemID(chrom_wdid_hg19, PROPS['chromosome'], is_qualifier=True), assembly_hg19]
        strand_orientation_hg19 = 'Q22809680' if genomic_pos_value_hg19['strand'] == 1 else 'Q22809711'
    else:
        do_hg19 = False
        strand_orientation_hg19 = None
        assembly_hg19 = None
        genomic_pos_ref_hg19 = None
        genomic_pos_value_hg19 = None
        qualifiers_hg19 = None
        chrom_wdid_hg19 = None

    s = []

    # strand orientation
    # if the same for both assemblies, only put one statement
    if do_hg19 and strand_orientation == strand_orientation_hg19:
        s.append(wdi_core.WDItemID(strand_orientation, PROPS['strand orientation'],
                                   references=[genomic_pos_ref], qualifiers=[assembly, assembly_hg19]))
    else:
        s.append(wdi_core.WDItemID(strand_orientation, PROPS['strand orientation'],
                                   references=[genomic_pos_ref], qualifiers=[assembly]))
        if do_hg19:
            s.append(wdi_core.WDItemID(strand_orientation_hg19, PROPS['strand orientation'],
                                       references=[genomic_pos_ref_hg19], qualifiers=[assembly_hg19]))

    # genomic start and end for both assemblies
    s.append(wdi_core.WDString(str(int(genomic_pos_value['start'])), PROPS['genomic start'],
                               references=[genomic_pos_ref], qualifiers=qualifiers))
    s.append(wdi_core.WDString(str(int(genomic_pos_value['end'])), PROPS['genomic end'],
                               references=[genomic_pos_ref], qualifiers=qualifiers))
    if do_hg19:
        s.append(wdi_core.WDString(str(int(genomic_pos_value_hg19['start'])), PROPS['genomic start'],
                                   references=[genomic_pos_ref_hg19], qualifiers=qualifiers_hg19))
        s.append(wdi_core.WDString(str(int(genomic_pos_value_hg19['end'])), PROPS['genomic end'],
                                   references=[genomic_pos_ref_hg19], qualifiers=qualifiers_hg19))

    # chromosome
    # if the same for both assemblies, only put one statement
    if do_hg19 and chrom_wdid == chrom_wdid_hg19:
        s.append(wdi_core.WDItemID(chrom_wdid, PROPS['chromosome'],
                                   references=[genomic_pos_ref], qualifiers=[assembly, assembly_hg19]))
    else:
        s.append(wdi_core.WDItemID(chrom_wdid, PROPS['chromosome'],
                                   references=[genomic_pos_ref], qualifiers=[assembly]))
        if do_hg19:
            s.append(wdi_core.WDItemID(chrom_wdid_hg19, PROPS['chromosome'],
                                       references=[genomic_pos_ref_hg19], qualifiers=[assembly_hg19]))

    return s


def do_gp_non_human(record, chr_num_wdid, external_ids, login=None):
    genomic_pos_value = record['genomic_pos']['@value']
    genomic_pos_source = record['genomic_pos']['@source']
    genomic_pos_id_prop = source_ref_id[genomic_pos_source['id']]
    genomic_pos_ref = make_ref_source(genomic_pos_source, PROPS[genomic_pos_id_prop],
                                      external_ids[genomic_pos_id_prop], login=login)

    # create qualifier for start/stop/orientation
    chrom_wdid = chr_num_wdid[genomic_pos_value['chr']]
    qualifiers = [wdi_core.WDItemID(chrom_wdid, PROPS['chromosome'], is_qualifier=True)]

    s = []
    # strand orientation
    strand_orientation = 'Q22809680' if genomic_pos_value['strand'] == 1 else 'Q22809711'
    s.append(wdi_core.WDItemID(strand_orientation, PROPS['strand orientation'], references=[genomic_pos_ref]))
    # genomic start and end
    s.append(wdi_core.WDString(str(int(genomic_pos_value['start'])), PROPS['genomic start'],
                               references=[genomic_pos_ref], qualifiers=qualifiers))
    s.append(wdi_core.WDString(str(int(genomic_pos_value['end'])), PROPS['genomic end'],
                               references=[genomic_pos_ref], qualifiers=qualifiers))
    # chromosome
    s.append(wdi_core.WDItemID(chrom_wdid, PROPS['chromosome'], references=[genomic_pos_ref]))

    return s


def main(coll: pymongo.collection.Collection, taxid: str, log_dir: str = "./logs", write: bool = True) -> None:
    """
    Main function for creating/updating genes

    :param coll: mongo collection containing gene data from mygene
    :param taxid: taxon to use (ncbi tax id)
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

    # make sure all chromosome items are found in wikidata
    if taxid not in organisms_info:
        raise ValueError("organism_info not found")
    organism_info = organisms_info[taxid]
    cb = ChromosomeBot()
    chr_num_wdid = cb.get_or_create(organism_info)

    # only do certain records
    docs = coll.find({'taxid': taxid, 'type_of_gene': 'protein-coding'})
    total = docs.count()
    docs = HelperBot.validate_docs(docs, PROPS['Entrez Gene ID'])
    records = HelperBot.tag_mygene_docs(docs)

    for record in tqdm(records, mininterval=2, total=total):
        create_item(record, organism_info, chr_num_wdid, login, write=write)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='run wikidata GO bot')
    parser.add_argument('--log-dir', help='directory to store logs', type=str)
    parser.add_argument('--dummy', help='do not actually do write', action='store_true')
    parser.add_argument('--taxon', help="only run using this taxon (ncbi tax id)", type=str)
    parser.add_argument('--mongo-uri', type=str, default="mongodb://localhost:27017")
    parser.add_argument('--mongo-db', type=str, default="wikidata_src")
    parser.add_argument('--mongo-coll', type=str, default="mygene")
    args = parser.parse_args()
    log_dir = args.log_dir if args.log_dir else "./logs"
    run_id = datetime.now().strftime('%Y%m%d_%H:%M')
    taxon = args.taxon
    coll = MongoClient(args.mongo_uri)[args.mongo_db][args.mongo_coll]

    log_name = '{}-{}.log'.format(__metadata__['name'], datetime.now().strftime('%Y%m%d_%H:%M'))
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(__metadata__),
                                        logger_name='go{}'.format(taxon))

    main(coll, taxon, log_dir=log_dir, write=not args.dummy)
