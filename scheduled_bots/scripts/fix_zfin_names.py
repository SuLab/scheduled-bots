"""
One off, hacky script to fix already created zebrafish genes with duplicate names
It finds all zebrafish genes with dupe names (from their mygene/entrezgene gene symbol), that already exist in wikipedia
and changes the label

In the future, zebrafish genes without a ZFIN ID will be skipped
"""

from scheduled_bots.geneprotein import GeneBot, ProteinBot, HelperBot
from scheduled_bots.geneprotein.GeneBot import WDPASS, WDUSER, organisms_info, PROPS
from scheduled_bots.geneprotein.ChromosomeBot import ChromosomeBot
from wikidataintegrator import wdi_helpers, wdi_login
from pymongo import MongoClient

from collections import Counter
from wikidataintegrator.wdi_helpers import id_mapper

def do_nothing(records):
    for record in records:
        yield record


def genes():
    entrez_wd = id_mapper("P351")

    login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS)

    coll = MongoClient().wikidata_src.mygene
    metadata_coll = MongoClient().wikidata_src.mygene_sources
    metadata = metadata_coll.find_one()
    organism_info = organisms_info[7955]

    doc_filter = {'taxid': 7955, 'entrezgene': {'$exists': True}}
    docs = coll.find(doc_filter).batch_size(20)
    total = docs.count()
    print("total number of records: {}".format(total))
    docs = HelperBot.validate_docs(docs, 'eukaryotic', PROPS['Entrez Gene ID'])
    records = HelperBot.tag_mygene_docs(docs, metadata)
    records = list(records)

    # find all names with dupes
    dupe_names = {k for k,v in Counter([x['symbol']['@value'] for x in records]).items() if v>1}

    # for all records that have one of these names, change the name to "name (entrezgene)"
    records = [x for x in records if x['symbol']['@value'] in dupe_names]
    for record in records:
        record['symbol']['@value'] = record['symbol']['@value'] + " (" + str(record['entrezgene']['@value']) + ")"

    # skip items that aren't already in wikidata (DONT CREATE NEW ITEMS!)
    records = [x for x in records if str(x['entrezgene']['@value']) in entrez_wd]

    print("len records: {}".format(len(records)))

    cb = ChromosomeBot()
    chr_num_wdid = cb.get_or_create(organism_info, login=login)
    bot = GeneBot.MammalianGeneBot(organism_info, chr_num_wdid, login)
    bot.filter = lambda x: iter(x)
    bot.run(records, total=total, fast_run=True, write=True)


def proteins():
    uni_wd = id_mapper("P352")

    login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS)

    coll = MongoClient().wikidata_src.mygene
    metadata_coll = MongoClient().wikidata_src.mygene_sources
    metadata = metadata_coll.find_one()
    organism_info = organisms_info[7955]

    doc_filter = {'taxid': 7955, 'uniprot': {'$exists': True}, 'entrezgene': {'$exists': True}}
    docs = coll.find(doc_filter).batch_size(20)
    total = docs.count()
    print("total number of records: {}".format(total))
    docs = HelperBot.validate_docs(docs, 'eukaryotic', PROPS['Entrez Gene ID'])
    records = HelperBot.tag_mygene_docs(docs, metadata)
    records = list(records)

    for record in records:
        if 'Swiss-Prot' in record['uniprot']['@value']:
            record['uniprot_id'] = record['uniprot']['@value']['Swiss-Prot']
        elif 'TrEMBL' in record['uniprot']['@value']:
            record['uniprot_id'] = record['uniprot']['@value']['TrEMBL']
    records = [x for x in records if 'uniprot_id' in x and isinstance(x['uniprot_id'], str)]

    # find all names with dupes
    dupe_names = {k for k,v in Counter([x['name']['@value'] for x in records]).items() if v>1}

    # for all records that have one of these names, change the name to "name (uniprot)"
    records = [x for x in records if x['name']['@value'] in dupe_names]
    print("len dupe records: {}".format(len(records)))

    for record in records:
        record['name']['@value'] = record['name']['@value'] + " (" + record['uniprot_id'] + ")"

    # skip items that aren't already in wikidata (DONT CREATE NEW ITEMS!)
    records = [x for x in records if x['uniprot_id'] in uni_wd]

    print("len records: {}".format(len(records)))

    cb = ChromosomeBot()
    chr_num_wdid = cb.get_or_create(organism_info, login=login)
    bot = ProteinBot.ProteinBot(organism_info, chr_num_wdid, login)
    bot.filter = lambda x: iter(x)
    bot.run(records, total=total, fast_run=False, write=True)

if __name__ == "__main__":
    #genes()
    proteins()
