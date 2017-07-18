"""
This actually does a write in Wikidata
"""
from wikidataintegrator import wdi_login, wdi_core, wdi_helpers

from scheduled_bots.geneprotein import HelperBot
from scheduled_bots.geneprotein.GeneBot import main, Gene, PROPS
from pymongo import MongoClient
from scheduled_bots.local import WDUSER, WDPASS


def test_make_gene_class():
    coll = MongoClient().wikidata_src.mygene
    metadata_coll = MongoClient().wikidata_src.mygene_sources
    metadata = metadata_coll.find_one()
    doc_filter = {'_id': '100861512'}
    docs = coll.find(doc_filter)
    print("total number of records: {}".format(coll.find(doc_filter).count()))

    validate_type = 'eukaryotic'
    docs = HelperBot.validate_docs(docs, validate_type, 'P351')
    records = HelperBot.tag_mygene_docs(docs, metadata)
    record = next(records)

    organism_info = {
        "name": "Homo sapiens",
        "type": "mammalian",
        "wdid": "Q15978631",
        'taxid': 9606
    }

    login = wdi_login.WDLogin(WDUSER, WDPASS)

    gene = Gene(record, organism_info, login)
    gene.create_item(fast_run=False, write=True)
    gene.remove_deprecated_statements()

def _test_write_one_gene(qid, entrezgene, taxid):
    coll = MongoClient().wikidata_src.mygene
    metadata_coll = MongoClient().wikidata_src.mygene_sources
    metadata = metadata_coll.find_one()
    doc_filter = {'_id': entrezgene}
    print("total number of records: {}".format(coll.find(doc_filter).count()))

    main(coll, taxid=taxid, metadata=metadata, fast_run=False, write=True, doc_filter=doc_filter)

    fn = wdi_core.WDItemEngine.logger.handlers[0].baseFilename
    log = open(fn).read()
    assert qid in log
    assert "WARNING" not in log and "ERROR" not in log


def test_write_one_human_gene():
    qid = "Q17915123"
    taxid = '9606'
    entrezgene = '7276'
    _test_write_one_gene(qid, entrezgene, taxid)


def test_write_one_microbe_gene():
    qid = "Q23124687"
    taxid = '243277'
    entrezgene = '2614876'
    _test_write_one_gene(qid, entrezgene, taxid)


def test_write_one_yeast_gene():
    qid = "Q27539996"
    taxid = '559292'
    entrezgene = '856002'
    _test_write_one_gene(qid, entrezgene, taxid)


def test_write_one_mouse_gene():
    qid = "Q18253743"
    taxid = '10090'
    entrezgene = '19744'
    _test_write_one_gene(qid, entrezgene, taxid)


def validate_all_human_genes():
    # runs all genes through the validator
    # and generates a log file

    coll = MongoClient().wikidata_src.mygene
    metadata_coll = MongoClient().wikidata_src.mygene_sources
    metadata = metadata_coll.find_one()
    doc_filter = {'taxid': 9606, 'entrezgene': {'$exists': True}}
    docs = coll.find(doc_filter)
    print("total number of records: {}".format(coll.find(doc_filter).count()))

    validate_type = 'eukaryotic'
    docs = HelperBot.validate_docs(docs, validate_type, 'P351')
    records = HelperBot.tag_mygene_docs(docs, metadata)

    _ = list(records)