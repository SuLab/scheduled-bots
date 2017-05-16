from .GeneBot import *


def test_write_one_human_gene():
    login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS)

    coll = MongoClient().wikidata_src.mygene
    metadata_coll = MongoClient().wikidata_src.mygene_sources
    metadata = metadata_coll.find_one()
    organism_info = organisms_info[9606]

    doc_filter = {'taxid': 9606, 'entrezgene': {'$exists': True}, '_id': '1107'}
    docs = coll.find(doc_filter).batch_size(20)
    total = docs.count()
    print("total number of records: {}".format(total))
    docs = HelperBot.validate_docs(docs, 'eukaryotic', PROPS['Entrez Gene ID'])
    records = HelperBot.tag_mygene_docs(docs, metadata)

    cb = ChromosomeBot()
    chr_num_wdid = cb.get_or_create(organism_info, login=login)
    bot = HumanGeneBot(organism_info, chr_num_wdid, login)
    bot.run(records, total=total, fast_run=False, write=False)