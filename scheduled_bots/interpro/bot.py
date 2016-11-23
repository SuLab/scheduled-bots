import argparse
import glob
import os
from dateutil.parser import parse as date_parse

from scheduled_bots.interpro import ItemsBot
from scheduled_bots.interpro import ProteinBot

from wikidataintegrator import wdi_core, wdi_login, wdi_helpers

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
    parser.add_argument('--run-one', help='run one doc', type=str)
    parser.add_argument('--dummy', help='do not actually do write', action='store_true')
    parser.add_argument('--interpro-version', type=str)
    parser.add_argument('--interpro-date', type=str)
    args = parser.parse_args()
    log_dir = args.log_dir if args.log_dir else "./logs"
    login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS)

    version_date = date_parse(args.interpro_date)
    version_num = args.interpro_version

    #version_date = date_parse("03-NOV-16")
    #version_num = "60.0"

    release = wdi_helpers.Release(title="InterPro Release {}".format(version_num),
                                  description="Release {} of the InterPro database & software".format(version_num),
                                  edition_of_wdid="Q3047275",
                                  edition=version_num,
                                  pub_date=version_date,
                                  archive_url="ftp://ftp.ebi.ac.uk/pub/databases/interpro/{}/".format(version_num))
    release_wdid = release.get_or_create(login)
    print("release_wdid: {}".format(release_wdid))

    ItemsBot.main(login, release_wdid, log_dir=log_dir, run_one=args.run_one, write=not args.dummy)
    ProteinBot.main(login, release_wdid, log_dir=log_dir, run_one=args.run_one, write=not args.dummy)

    for file_path in glob.glob(os.path.join(log_dir, "*.log")):
        # bot_log_parser.process_log(file_path)
        pass
