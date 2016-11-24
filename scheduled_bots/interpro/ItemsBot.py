import json
import os
from datetime import datetime


from wikidataintegrator import wdi_core, wdi_login, wdi_helpers
from pymongo import MongoClient
from tqdm import tqdm

from .IPRTerm import IPRTerm

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
    cursor = interpro_coll.find(no_cursor_timeout=True)
    for doc in tqdm(cursor, total=cursor.count(), mininterval=1.0):
        doc['release_wdid'] = release_wdid
        term = IPRTerm(**doc)
        term.create_item(login, write=write)
        terms.append(term)
        if run_one:
            break
    cursor.close()

    # create/update interpro item relationships
    IPRTerm.refresh_ipr_wd()
    for term in tqdm(terms, mininterval=1.0):
        term.create_relationships(login, write=write)

    return os.path.join(log_dir, log_name)