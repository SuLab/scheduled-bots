## we have a list of rows, some need to be deleted
from tqdm import tqdm

from scheduled_bots import PROPS
from wikidataintegrator import wdi_core, wdi_helpers, wdi_login
from scheduled_bots.local import WDUSER, WDPASS
import pandas as pd

# the following is for omim
# df gotten from: https://docs.google.com/spreadsheets/d/1NMDWomzQzjFwL_9B-3up0qSKzqmmTg_Orrqt-pQg0vU/edit#gid=1156925698

df = pd.read_csv("Wikidata DO xrefs OMIM - no match.csv")
df = df.fillna("")
records = df.to_dict("records")
login = wdi_login.WDLogin(WDUSER, WDPASS)

# value = "604159"
# doid = "DOID:0060695"
# qid = "https://www.wikidata.org/wiki/Q1781802"

# delete susceptibility
dfs = df[df['susceptibility to'].str.startswith("x")]
for record in tqdm(dfs.to_dict("records")):
    value = record['DbXref'].split(":")[1]
    doid = record['ID']
    qid = record['QID']
    qid = qid.split("/")[-1]

    s = wdi_core.WDExternalID(value, PROPS['OMIM ID'])
    setattr(s, 'remove', '')

    item = wdi_core.WDItemEngine(wd_item_id=qid, data=[s])
    item.write(login, edit_summary="remove omim susceptibility from disease items")

# remove from wikidata
dfs = df[df['Remove from WD'].str.startswith("x")]
for record in tqdm(dfs.to_dict("records")):
    value = record['DbXref'].split(":")[1]
    doid = record['ID']
    qid = record['QID']
    qid = qid.split("/")[-1]

    s = wdi_core.WDExternalID(value, PROPS['OMIM ID'])
    setattr(s, 'remove', '')

    item = wdi_core.WDItemEngine(wd_item_id=qid, data=[s])
    item.write(login, edit_summary="remove incorrect omim ids from disease items")

# add mapping relation type
dfs = df[df['narrowMatch'].str.startswith("x")]
for record in tqdm(dfs.to_dict("records")):
    value = record['DbXref'].split(":")[1]
    doid = record['ID']
    qid = record['QID']
    qid = qid.split("/")[-1]
    mrh = wdi_helpers.MappingRelationHelper()
    s = wdi_core.WDExternalID(value, PROPS['OMIM ID'])
    mrh.set_mrt(s, "narrow")

    item = wdi_core.WDItemEngine(wd_item_id=qid, data=[s], append_value=[PROPS['OMIM ID']])
    item.write(login)
