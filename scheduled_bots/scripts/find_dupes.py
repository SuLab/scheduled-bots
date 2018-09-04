# find other items with the same label
import requests
from pprint import pprint
from tqdm import tqdm

from wikidataintegrator import wdi_helpers, wdi_core, wdi_login
from scheduled_bots.local import GREGUSER, GREGPASS


def search(search_string):
    params = {
        'action': 'wbsearchentities',
        'language': 'en',
        'search': search_string,
        'format': 'json',
        'limit': 3
    }
    mediawiki_api_url = 'https://www.wikidata.org/w/api.php'
    reply = requests.get(mediawiki_api_url, params=params)
    reply.raise_for_status()
    search_results = reply.json()
    return search_results['search']


def create_redirect(from_id, to_id, login):
    params = {
        'action': 'wbeditentity',
        'id': from_id,
        'clear': True,
        'summary': 'Clearing item to prepare for redirect',
        'data': '{}',
        'format': 'json',
        'token': login.get_edit_token()
    }
    url = 'https://www.wikidata.org/w/api.php'
    clear_reply = requests.post(url=url, data=params, cookies=login.get_edit_cookie())
    clear_reply.raise_for_status()
    d = clear_reply.json()
    if "error" in d:
        raise ValueError(d)

    params = {
        'action': 'wbcreateredirect',
        'from': from_id,
        'to': to_id,
        'token': login.get_edit_token(),
        'format': 'json',
    }
    url = 'https://www.wikidata.org/w/api.php'
    merge_reply = requests.post(url=url, data=params, cookies=login.get_edit_cookie())
    merge_reply.raise_for_status()
    d = merge_reply.json()
    if "error" in d:
        raise ValueError(d)


login = wdi_login.WDLogin(GREGUSER, GREGPASS)
log = open("log.txt", "w")
disease_qids = wdi_helpers.id_mapper("P699")

for disease_qid in tqdm(sorted(list(disease_qids.values()))):
    item = wdi_core.WDItemEngine(wd_item_id=disease_qid)
    if not len(item.statements) == 1:
        continue

    label = item.get_label()

    search_results = search(label)
    search_results = [x for x in search_results if x['description'] != "scientific article"]
    search_results = [x for x in search_results if x['id'] != disease_qid]
    search_results = [x for x in search_results if x['label'].lower() == label.lower()]

    if len(search_results) != 1:
        continue
    to_merge_qid = search_results[0]['id']
    wdi_core.WDItemEngine.merge_items(disease_qid, to_merge_qid, login_obj=login, ignore_conflicts='description')
    create_redirect(disease_qid, to_merge_qid, login)
    s = [disease_qid, to_merge_qid, label, search_results[0]['label'],
         "http://www.wikidata.org/entity/" + disease_qid, "http://www.wikidata.org/entity/" + to_merge_qid]
    print("|".join(s), file=log)
    print(" | ".join(s))
