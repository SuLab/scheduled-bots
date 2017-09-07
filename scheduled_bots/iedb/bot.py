import argparse
import json
import os
from datetime import datetime
from time import gmtime, strftime
import pandas as pd
import requests
from tqdm import tqdm

from wikidataintegrator import wdi_core, wdi_helpers, wdi_login, ref_handlers
from wikidataintegrator.wdi_helpers import id_mapper

try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

PROPS = {'IEDB Epitope ID': 'P4168',
         'ChEBI-ID': 'P683',
         'stated in': 'P248',
         'retrieved': 'P813',
         }

__metadata__ = {'name': 'IEDB Epitope Bot', 'tags': ['drugs'], 'properties': list(PROPS.values())}


def create_references(iedb_id):
    reference = [wdi_core.WDItemID(value='Q1653430', prop_nr=PROPS['stated in'], is_reference=True),  # Stated in IEDB
                 wdi_core.WDExternalID(value=iedb_id, prop_nr=PROPS['IEDB Epitope ID'], is_reference=True)]
    t = strftime("+%Y-%m-%dT00:00:00Z", gmtime())
    reference.append(wdi_core.WDTime(t, prop_nr=PROPS['retrieved'], is_reference=True))
    return [reference]


def main(chebi_iedb_map, log_dir="./logs", fast_run=False, write=True):
    login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS)
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, logger_name='WD_logger', log_name=log_name,
                                        header=json.dumps(__metadata__))

    chebi_qid_map = id_mapper(PROPS['ChEBI-ID'])

    for chebi, iedb in tqdm(chebi_iedb_map.items()):
        if chebi not in chebi_qid_map:
            msg = wdi_helpers.format_msg(iedb, PROPS['IEDB Epitope ID'], None, "ChEBI:{} not found".format(chebi), "ChEBI not found")
            print(msg)
            wdi_core.WDItemEngine.log("WARNING", msg)
            continue
        s = [wdi_core.WDExternalID(iedb, PROPS['IEDB Epitope ID'], references=create_references(iedb))]
        item = wdi_core.WDItemEngine(wd_item_id=chebi_qid_map[chebi], data=s, domain="drugs", fast_run=fast_run,
                                     fast_run_base_filter={PROPS['ChEBI-ID']: ''}, fast_run_use_refs=True,
                                     ref_handler=ref_handlers.update_retrieved_if_new)
        wdi_helpers.try_write(item, iedb, PROPS['IEDB Epitope ID'], login, edit_summary="Add IEDB Epitope ID",
                              write=write)


if __name__ == "__main__":
    """
    Bot to add/update IEDB xrefs to Wikidata
    """
    parser = argparse.ArgumentParser(description='run bot')
    parser.add_argument('--path', help='path to file')
    parser.add_argument('--url', help='url to file')
    parser.add_argument('--log-dir', help='directory to store logs', type=str)
    parser.add_argument('--dummy', help='do not actually do write', action='store_true')
    parser.add_argument('--fastrun', dest='fastrun', action='store_true')
    parser.add_argument('--no-fastrun', dest='fastrun', action='store_false')
    parser.set_defaults(fastrun=True)

    args = parser.parse_args()
    if (args.path and args.url) or not (args.path or args.url):
        raise ValueError("must give one of --path and --url")
    log_dir = args.log_dir if args.log_dir else "./logs"
    run_id = datetime.now().strftime('%Y%m%d_%H:%M')
    __metadata__['run_id'] = run_id
    fast_run = args.fastrun

    log_name = '{}-{}.log'.format(__metadata__['name'], run_id)
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(__metadata__),
                                        logger_name='gene-disease')

    path = args.url if args.url else args.path
    df = pd.read_csv(path, names=['chebi', 'iedb'], sep="|")
    chebi_iedb_map = dict(zip(df.chebi, df.iedb))
    chebi_iedb_map = {k.replace("CHEBI:", ""): v.replace("http://www.iedb.org/epitope/", "") for k, v in
                      chebi_iedb_map.items()}
    main(chebi_iedb_map, log_dir=log_dir, fast_run=fast_run, write=not args.dummy)
