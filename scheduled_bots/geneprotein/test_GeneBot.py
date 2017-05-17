"""
This actually does a write in Wikidata
"""

from scheduled_bots.geneprotein.GeneBot import main, wdi_core
from pymongo import MongoClient


def test_write_one_human_gene():
    # https://www.wikidata.org/wiki/Q14911732
    coll = MongoClient().wikidata_src.mygene
    metadata_coll = MongoClient().wikidata_src.mygene_sources
    metadata = metadata_coll.find_one()
    doc_filter = {'_id': '1017'}
    print("total number of records: {}".format(coll.find(doc_filter).count()))

    main(coll, taxid='9606', metadata=metadata, fast_run=False, write=True, doc_filter=doc_filter)

    fn = wdi_core.WDItemEngine.logger.handlers[0].baseFilename
    log = open(fn).read()
    assert "Q14911732" in log
    assert "WARNING" not in log and "ERROR" not in log



def test_write_one_microbe_gene():
    # https://www.wikidata.org/wiki/Q23124687
    coll = MongoClient().wikidata_src.mygene
    metadata_coll = MongoClient().wikidata_src.mygene_sources
    metadata = metadata_coll.find_one()
    doc_filter = {'_id': '2614876'}
    print("total number of records: {}".format(coll.find(doc_filter).count()))

    main(coll, taxid='243277', metadata=metadata, fast_run=False, write=True, doc_filter=doc_filter)

    fn = wdi_core.WDItemEngine.logger.handlers[0].baseFilename
    log = open(fn).read()
    assert "Q23124687" in log
    assert "WARNING" not in log and "ERROR" not in log


def test_write_one_yeast_gene():
    # https://www.wikidata.org/wiki/Q27539996
    coll = MongoClient().wikidata_src.mygene
    metadata_coll = MongoClient().wikidata_src.mygene_sources
    metadata = metadata_coll.find_one()
    doc_filter = {'_id': '856002'}
    print("total number of records: {}".format(coll.find(doc_filter).count()))

    main(coll, taxid='559292', metadata=metadata, fast_run=False, write=True, doc_filter=doc_filter)

    fn = wdi_core.WDItemEngine.logger.handlers[0].baseFilename
    log = open(fn).read()
    assert "Q27539996" in log
    assert "WARNING" not in log and "ERROR" not in log