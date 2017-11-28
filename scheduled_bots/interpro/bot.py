"""
USe api: http://www.uniprot.org/uniprot/?query=organism%3A%22Homo+sapiens+%28Human%29+%5B9606%5D%22&sort=score
import requests

cookies = {
    'uniprot-results-download': 'tab',
    'insd-target-sequences': 'embl',
    'insd_cds-target-sequences': 'embl_cds',
    'insd-target-cross_references': 'embl',
    'insd_cds-target-cross_references': 'embl_cds',
    'uniprot-columns2': 'id%2Centry_name%2Creviewed%2Cprotein_names%2Cgenes%2Corganism%2Clength%2Cdatabase%28InterPro%29',
    '_ga': 'GA1.2.12501074.1488310603',
    '_gid': 'GA1.2.1809071125.1511808535',
    '_gat': '1',
}

headers = {
    'DNT': '1',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'en-US,en;q=0.8',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Referer': 'http://www.uniprot.org/uniprot/?query=organism:9606+keyword:181',
    'Connection': 'keep-alive',
}

params = (
    ('sort', ''),
    ('desc', ''),
    ('compress', 'yes'),
    ('query', 'organism:9606 keyword:181'),
    ('fil', ''),
    ('format', 'tab'),
    ('force', 'yes'),
    ('columns', 'id,entry name,reviewed,protein names,genes,organism,length,database(InterPro)'),
)

requests.get('http://www.uniprot.org/uniprot/', headers=headers, params=params, cookies=cookies)

#NB. Original query string below. It seems impossible to parse and
#reproduce query strings 100% accurately so the one below is given
#in case the reproduced version is not "correct".
# requests.get('http://www.uniprot.org/uniprot/?sort=&desc=&compress=yes&query=organism:9606%20keyword:181&fil=&format=tab&force=yes&columns=id,entry%20name,reviewed,protein%20names,genes,organism,length,database(InterPro)', headers=headers, cookies=cookies)



"""


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