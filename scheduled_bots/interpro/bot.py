import argparse
import os

import gc
from dateutil.parser import parse as date_parse

from scheduled_bots.interpro import ItemsBot, get_all_taxa
from scheduled_bots.interpro import ProteinBot

from wikidataintegrator import wdi_login, wdi_helpers, wdi_core

try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='run interpro wikidata import bot')
    parser.add_argument('--log-dir', help='directory to store logs', type=str)
    parser.add_argument('--run-one', help='run one doc', action='store_true')
    parser.add_argument('--dummy', help='do not actually do write', action='store_true')
    parser.add_argument('--taxon', help='limit protein -> interpro to taxon (as a qid)', type=str)
    parser.add_argument('--interpro-version', help="example: '60.0'", type=str)
    parser.add_argument('--interpro-date', help="format example: '03-NOV-16'", type=str)
    parser.add_argument('--protein', help='run protein ipr bot', action='store_true')
    parser.add_argument('--items', help='run item ipr bot', action='store_true')
    parser.add_argument('--mongo-uri', type=str, default="mongodb://localhost:27017")
    parser.add_argument('--mongo-db', type=str, default="wikidata_src")

    args = parser.parse_args()
    if not (args.protein or args.items):
        args.protein = args.items = True

    mongo_db = args.mongo_db
    mongo_uri = args.mongo_uri

    log_dir = args.log_dir if args.log_dir else "./logs"
    login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS)

    version_date = date_parse(args.interpro_date)
    version_num = args.interpro_version

    release = wdi_helpers.Release(title="InterPro Release {}".format(version_num),
                                  description="Release {} of the InterPro database & software".format(version_num),
                                  edition_of_wdid="Q3047275",
                                  edition=version_num,
                                  pub_date=version_date,
                                  archive_url="ftp://ftp.ebi.ac.uk/pub/databases/interpro/{}/".format(version_num))
    release_wdid = release.get_or_create(login)
    print("release_wdid: {}".format(release_wdid))

    if args.items:
        print("running item bot")
        ItemsBot.main(login, release_wdid, mongo_db=mongo_db, mongo_uri=mongo_uri,
                      log_dir=log_dir, run_one=args.run_one, write=not args.dummy)

    if args.protein:
        print("protein ipr bot")
        if args.taxon:
            taxa = args.taxon.split(",")
        else:
            taxa = get_all_taxa()

        for taxon in taxa:
            print("running protein ipr bot on taxon: {}".format(taxon))
            ProteinBot.main(login, release_wdid, taxon=taxon, mongo_db=mongo_db, mongo_uri=mongo_uri,
                            log_dir=log_dir, run_one=args.run_one, write=not args.dummy)
            for frc in wdi_core.WDItemEngine.fast_run_store:
                frc.clear()
            wdi_core.WDItemEngine.fast_run_store = []
            gc.collect()


    print("DONE")