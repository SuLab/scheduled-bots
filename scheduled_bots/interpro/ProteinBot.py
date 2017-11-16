import json
import os
from datetime import datetime
import time
from pymongo import MongoClient
from tqdm import tqdm
from wikidataintegrator import wdi_core, wdi_helpers

from scheduled_bots.interpro import remove_deprecated_statements
from scheduled_bots.interpro.IPRTerm import IPRTerm
from wikidataintegrator.ref_handlers import update_retrieved_if_new
from wikidataintegrator.wdi_fastrun import FastRunContainer

__metadata__ = {'name': 'InterproBot_Proteins',
                'maintainer': 'GSS',
                'tags': ['protein', 'interpro'],
                'properties': ["P279", "P527", "P361"]
                }

INTERPRO = "P2926"
UNIPROT = "P352"

PROPS = {
    "subclass of": "P279",
    "has part": "P527",
    "part of": "P361"
}


def create_for_one_protein(login, doc, release_wdid, uniprot2wd, fast_run_base_filter, write=True):
    uniprot_id = doc['_id']
    try:
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
                # changed subclass to part of
                # https://github.com/SuLab/GeneWikiCentral/issues/68
                statements.append(wdi_core.WDItemID(IPRTerm.ipr2wd[f], PROPS['part of'], references=[reference]))
        if doc['has_part']:
            for hp in doc['has_part']:
                statements.append(wdi_core.WDItemID(IPRTerm.ipr2wd[hp], PROPS['has part'], references=[reference]))

        if uniprot_id not in uniprot2wd:
            print("wdid_not_found " + uniprot_id + " " + uniprot2wd[uniprot_id])
            wdi_core.WDItemEngine.log("ERROR", wdi_helpers.format_msg(uniprot_id, UNIPROT, None, "wdid_not_found"))

        wd_item = wdi_core.WDItemEngine(wd_item_id=uniprot2wd[uniprot_id], domain="proteins", data=statements,
                                        fast_run=True, fast_run_base_filter=fast_run_base_filter,
                                        append_value=[PROPS['has part'], PROPS['part of']],
                                        global_ref_mode='CUSTOM', fast_run_use_refs=True,
                                        ref_handler=update_retrieved_if_new)
    except Exception as e:
        wdi_core.WDItemEngine.log("ERROR",
                                  wdi_helpers.format_msg(uniprot_id, UNIPROT, uniprot2wd[uniprot_id], str(e),
                                                         msg_type=type(e)))
        return None

    wdi_helpers.try_write(wd_item, uniprot_id, INTERPRO, login, write=write,
                          edit_summary="add/update family and/or domains")
    wd_item.wd_json_representation = dict()
    wd_item.statements = []
    return wd_item


def create_uniprot_relationships(login, release_wdid, collection, taxon=None, write=True):
    # only do uniprot proteins that are already in wikidata
    # returns list of qids of items that are modified or skipped (excluding created)
    if taxon:
        uniprot2wd = wdi_helpers.id_mapper(UNIPROT, (("P703", taxon),))
        fast_run_base_filter = {UNIPROT: "", "P703": taxon}
    else:
        uniprot2wd = wdi_helpers.id_mapper(UNIPROT)
        fast_run_base_filter = {UNIPROT: ""}

    cursor = collection.find({'_id': {'$in': list(uniprot2wd.keys())}}).batch_size(20)
    qids = []
    for n, doc in tqdm(enumerate(cursor), total=cursor.count(), mininterval=10.0):
        wd_item = create_for_one_protein(login, doc, release_wdid, uniprot2wd, fast_run_base_filter, write=write)
        if wd_item and not wd_item.create_new_item:
            qids.append(wd_item.wd_item_id)
    return qids


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
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(__metadata__),
                                        logger_name='ipr{}'.format(taxon))

    qids = create_uniprot_relationships(login, release_wdid, collection, taxon=taxon, write=write)
    for frc in wdi_core.WDItemEngine.fast_run_store:
        frc.clear()

    if not taxon:
        print("cant remove deprecated statements without specifying taxon")
        return os.path.join(log_dir, log_name)
    time.sleep(10 * 60)  # sleep for 10 min so (hopefully) the wd sparql endpoint updates

    frc = FastRunContainer(wdi_core.WDBaseDataType, wdi_core.WDItemEngine, base_filter={UNIPROT: "", "P703": taxon}, use_refs=True)
    for qid in qids:
        # the old subclass of statements should get removed here (see: https://github.com/SuLab/GeneWikiCentral/issues/68)
        remove_deprecated_statements(qid, frc, release_wdid, [PROPS[x] for x in ['subclass of', 'has part', 'part of']], login)

    return os.path.join(log_dir, log_name)
