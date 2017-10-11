import json
import os
from datetime import datetime
import time

from wikidataintegrator import wdi_core, wdi_login, wdi_helpers
from pymongo import MongoClient
from tqdm import tqdm

from scheduled_bots.interpro import remove_deprecated_statements
from scheduled_bots.interpro.IPRTerm import IPRTerm, INTERPRO
from wikidataintegrator.wdi_fastrun import FastRunContainer

__metadata__ = {'name': 'InterproBot_Items',
                'maintainer': 'GSS',
                'tags': ['interpro'],
                'properties': ["P279", "P2926", 'P527', 'P361']
                }


def main(login, release_wdid, log_dir="./logs", run_id=None, mongo_uri="mongodb://localhost:27017",
         mongo_db="wikidata_src", mongo_coll="interpro", run_one=False, write=True):
    # data sources
    db = MongoClient(mongo_uri)[mongo_db]
    interpro_coll = db[mongo_coll]

    if run_id is None:
        run_id = datetime.now().strftime('%Y%m%d_%H:%M')
    if log_dir is None:
        log_dir = "./logs"
    __metadata__['run_id'] = run_id
    __metadata__['timestamp'] = str(datetime.now())
    __metadata__['release'] = {
        'InterPro': {'release': None, '_id': 'InterPro', 'wdid': release_wdid, 'timestamp': None}}

    log_name = '{}-{}.log'.format(__metadata__['name'], __metadata__['run_id'])
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(__metadata__))

    # create/update all interpro items
    terms = []
    cursor = interpro_coll.find().batch_size(20)
    for n, doc in tqdm(enumerate(cursor), total=cursor.count(), mininterval=2.0):
        doc['release_wdid'] = release_wdid
        term = IPRTerm(**doc)
        term.create_item(login, write=write)
        terms.append(term)
        if run_one:
            break

    time.sleep(10*60)  # sleep for 10 min so (hopefully) the wd sparql endpoint updates

    # create/update interpro item relationships
    IPRTerm.refresh_ipr_wd()
    for term in tqdm(terms, mininterval=2.0):
        term.create_relationships(login, write=write)

    time.sleep(10 * 60)  # sleep for 10 min so (hopefully) the wd sparql endpoint updates

    print("remove deprecated statements")
    # first remove the fastrun stores so it has to be re-updated
    for term in terms:
        term.wd_item.fast_run_container.clear()
    frc = FastRunContainer(wdi_core.WDBaseDataType, wdi_core.WDItemEngine, base_filter={INTERPRO: ''}, use_refs=True)
    for term in tqdm(terms, mininterval=2.0):
        remove_deprecated_statements(term.wd_item.wd_item_id, frc, release_wdid, ["P279", "P2926", 'P527', 'P361'], login)

    return os.path.join(log_dir, log_name)