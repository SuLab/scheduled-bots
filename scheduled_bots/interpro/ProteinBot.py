import json
import os
from datetime import datetime

from wikidataintegrator import wdi_core, wdi_login, wdi_helpers
from dateutil.parser import parse as date_parse
from pymongo import MongoClient
from tqdm import tqdm


from .IPRTerm import IPRTerm

__metadata__ = {'name': 'InterproBot_Proteins',
                'maintainer': 'GSS',
                'tags': ['protein', 'interpro'],
                'properties': ["P279", "P527", "P361"]
                }

INTERPRO = "P2926"
UNIPROT = "P352"


def create_uniprot_relationships(login, release_wdid, collection, taxon=None, write=True):
    # only do uniprot proteins that are already in wikidata
    if taxon:
        uniprot2wd = wdi_helpers.id_mapper(UNIPROT, (("P703", taxon),))
        fast_run_base_filter = {UNIPROT: "", "P703": taxon}
    else:
        uniprot2wd = wdi_helpers.id_mapper(UNIPROT)
        fast_run_base_filter = {UNIPROT: ""}

    cursor = collection.find({'_id': {'$in': list(uniprot2wd.keys())}}, no_cursor_timeout=True)
    for doc in tqdm(cursor, total=cursor.count()):
        uniprot_id = doc['_id']
        statements = []
        # uniprot ID. needed for PBB_core to find uniprot item
        # statements.append(PBB_Core.WDExternalID(value=uniprot_id, prop_nr=UNIPROT))

        ## References
        # stated in Interpro version XX.X
        ref_stated_in = wdi_core.WDItemID(release_wdid, 'P248', is_reference=True)
        ref_ipr = wdi_core.WDString("http://www.ebi.ac.uk/interpro/protein/{}".format(uniprot_id), "P854",
                                    is_reference=True)
        reference = [ref_stated_in, ref_ipr]

        if doc['subclass']:
            for f in doc['subclass']:
                statements.append(wdi_core.WDItemID(value=IPRTerm.ipr2wd[f], prop_nr='P279', references=[reference]))
        if doc['has_part']:
            for hp in doc['has_part']:
                statements.append(wdi_core.WDItemID(value=IPRTerm.ipr2wd[hp], prop_nr='P527', references=[reference]))

        if uniprot_id not in uniprot2wd:
            print("wdid_not_found " + uniprot_id + " " + uniprot2wd[uniprot_id])
            wdi_core.WDItemEngine.log("ERROR", wdi_helpers.format_msg(uniprot_id, UNIPROT, None, "wdid_not_found"))

        wd_item = wdi_core.WDItemEngine(wd_item_id=uniprot2wd[uniprot_id], domain="proteins", data=statements,
                                        fast_run=True, fast_run_base_filter=fast_run_base_filter,
                                        append_value=["P279", "P527", "P361"])

        if wd_item.create_new_item:
            raise ValueError("something bad happened")
        wdi_helpers.try_write(wd_item, uniprot_id, INTERPRO, login, write=write,
                              edit_summary="add/update family and/or domains")

    cursor.close()


def main(login, release_wdid, log_dir="./logs", run_id=None, mongo_uri="mongodb://localhost:27017",
         mongo_db="wikidata_src", mongo_coll="interpro_protein", taxon=None, run_one=False, write=True):
    # data sources
    db = MongoClient(mongo_uri)[mongo_db]
    collection = db[mongo_coll]

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

    create_uniprot_relationships(login, release_wdid, collection, taxon=taxon, write=write)

    return os.path.join(log_dir, log_name)
