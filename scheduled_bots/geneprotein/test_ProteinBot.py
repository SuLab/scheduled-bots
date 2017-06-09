"""
This actually does a write in Wikidata
"""
from wikidataintegrator import wdi_login, wdi_core, wdi_helpers

from scheduled_bots.geneprotein import HelperBot
from scheduled_bots.geneprotein.ProteinBot import main, Protein, PROPS
from pymongo import MongoClient
from scheduled_bots.local import WDUSER, WDPASS

def _test_write_one_protein(qid, entrezgene, taxid):
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


def test_write_one_human_protein():
    qid = "Q21109414"
    taxid = '9606'
    entrezgene = '1877'
    _test_write_one_protein(qid, entrezgene, taxid)


def test_write_one_microbe_protein():
    qid = "Q23433065"
    taxid = '243277'
    entrezgene = '2614876'
    _test_write_one_protein(qid, entrezgene, taxid)


def test_write_another_microbe_protein():
    qid = "Q30106073"
    taxid = '243161'
    entrezgene = '1246473'
    _test_write_one_protein(qid, entrezgene, taxid)


def test_write_one_yeast_protein():
    qid = "Q27547347"
    taxid = '559292'
    entrezgene = '856002'
    _test_write_one_protein(qid, entrezgene, taxid)


def test_write_one_mouse_protein():
    qid = "Q21990557"
    taxid = '10090'
    entrezgene = '19744'
    _test_write_one_protein(qid, entrezgene, taxid)


def validate_all_human_protein():
    # runs all proteins through the validator
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