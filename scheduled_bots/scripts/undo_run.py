# undo bad things from a log file, or a list of revids

import pandas as pd
from tqdm import tqdm
import requests
from wikidataintegrator.wdi_login import WDLogin
from wikidataintegrator.wdi_core import WDItemEngine
from scheduled_bots.local import GREGUSER, GREGPASS

login = WDLogin(GREGUSER, GREGPASS)
log_path = "/home/gstupp/projects/wikidata-biothings/scheduled_bots/scheduled_bots/ontology/logs/Monarch Disease Ontology-20180727_17:11.log"


# from a sparql query, get a list of items that we want to undo edits from
query = """
select ?item (count(*) as ?c) where {
  SELECT DISTINCT ?item ?mondo WHERE {
    ?item wdt:P5270 ?mondo
  }
} group by ?item
having (?c > 1)
order by desc(?c)"""
item_df = WDItemEngine.execute_sparql_query(query, as_dataframe=True)
item_df.item = item_df.item.str.replace("http://www.wikidata.org/entity/", "")
items = list(item_df.item)

# read in a log of the run
def parse_log(file_path):
    df = pd.read_csv(file_path, sep=",",
                     names=['Level', 'Timestamp', 'External ID', 'Prop', 'QID', 'Message', 'Msg Type', 'Rev ID'],
                     skiprows=2, dtype={'External ID': str, 'Rev ID': str},
                     comment='#', quotechar='"', skipinitialspace=True, delimiter=';')
    df.fillna('', inplace=True)
    df.replace("None", "", inplace=True)
    df = df.apply(lambda x: x.str.strip())
    df.Timestamp = pd.to_datetime(df.Timestamp, format='%m/%d/%Y %H:%M:%S')
    return df

#########################
# Undo each edit from the log individually
#########################

df = parse_log(log_path)
df = df[df.QID.isin(items)]
df = df.sort_values("Timestamp")
df.dropna(subset=["QID"], inplace=True)
df.dropna(subset=["Rev ID"], inplace=True)
# take only the LAST revisions for a QID. AKA, keep the first revision for an item
df = df[df.duplicated("QID")]

print("{} edits to undo".format(len(df)))
records = df[['Rev ID', 'QID']].to_dict("records")
records = records[::-1]

for record in tqdm(records):
    qid = record['QID']
    revid = record['Rev ID']
    # qid = "Q183130"
    # revid = "717075555"

    params = {
        "action": "edit",
        "title": qid,
        "undo": revid,
        "token": login.get_edit_token(),
        "format": "json"
    }
    url = "https://www.wikidata.org/w/api.php"

    r = login.s.post(url, data=params)
    response = r.json()
    record['response'] = response
    if "error" in response:
        print(response)
        record['success'] = False
    else:
        print(response)
        record['success'] = True

#########################
# Copy of above, but instead, perform a "revert" by undoing all edits from the specified one in the log
# until the most recent, current edit. Be careful if there were edits since the run was performed
#########################

log_path = "/home/gstupp/projects/wikidata-biothings/scheduled_bots/scheduled_bots/ontology/logs/mondo/mondo.log"
df = parse_log(log_path)
df = df[df.QID.isin(items)]
df = df.sort_values("Timestamp")
df.dropna(subset=["QID"], inplace=True)
df.dropna(subset=["Rev ID"], inplace=True)
# take only the FIRST revision
df = df[~df.duplicated("QID")]

qid_revid = dict(zip(df['QID'], df['Rev ID']))

for qid in tqdm(items[200:]):
    revid = qid_revid[qid]
    # get the revisions for a page
    params = {
        'rvdir': 'older',
        'rvlimit': 2,
        'action': 'query',
        'prop': 'revisions',
        'titles': qid,
        'rvstartid': revid,
        'format': 'json'
    }
    url = "https://www.wikidata.org/w/api.php"
    p = requests.get(url, params=params).json()['query']['pages']
    last_revision = list(p.values())[0]['revisions'][-1]['revid']

    # get current revision
    params = {
        'rvlimit': 1,
        'action': 'query',
        'prop': 'revisions',
        'titles': qid,
        'format': 'json'
    }
    url = "https://www.wikidata.org/w/api.php"
    p = requests.get(url, params=params).json()['query']['pages']
    current_revision = list(p.values())[0]['revisions'][-1]['revid']

    params = {
        "action": "edit",
        "title": qid,
        "undoafter": last_revision,
        "undo": current_revision,
        "token": login.get_edit_token(),
        "format": "json",
        "summary": "‎Restore revision {}".format(last_revision)
    }
    url = "https://www.wikidata.org/w/api.php"

    r = login.s.post(url, data=params)
    response = r.json()
    record['response'] = response
    if "error" in response:
        print(response)
        record['success'] = False
    else:
        print(response)
        record['success'] = True