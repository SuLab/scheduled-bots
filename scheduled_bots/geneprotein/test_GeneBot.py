"""
This actually does a write in Wikidata
"""

from scheduled_bots.geneprotein.GeneBot import main, wdi_core
from pymongo import MongoClient


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
    qid = "Q14911732"
    taxid = '9606'
    entrezgene = '1017'
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
    qid = "Q18309250"
    taxid = '10090'
    entrezgene = '12566'
    _test_write_one_gene(qid, entrezgene, taxid)
