# get drugs from mesh (only looking at drugs that have a pharm action)
# add mesh ID to wikidata by matching up with xrefs in mesh (unii, cas, or ec)
# drugs could be small molecules, protiens, biological entities

import pandas as pd
import time

pd.set_option("display.width", 220)
import re

from tqdm import tqdm

from scheduled_bots.drugs.mesh_pa.dumper import get_drug_pa
from wikidataintegrator import wdi_helpers, wdi_core, wdi_login, ref_handlers
from scheduled_bots.local import WDPASS, WDUSER

PROPS = {'MeSH ID': 'P486',
         'MeSH Code': 'P672',
         'subclass': 'P279',
         'has role': 'P2868'}
ITEMS = {'Medical Subject Headings': 'Q199897'}
login = wdi_login.WDLogin(WDUSER, WDPASS)


#################
## get drugs by registry numbers. add mesh IDs
#################

def infer_type(s):
    if len(s) == 10 and '-' not in s:
        # https://www.wikidata.org/wiki/Property:P652
        return 'unii', s
    if re.fullmatch("[1-9]\d{1,6}-\d{2}-\d", s):
        # https://www.wikidata.org/wiki/Property:P231
        return 'cas', s
    if s.startswith("EC "):
        # https://www.wikidata.org/wiki/Property:P591
        return "ec", s.replace("EC ", "")
    else:
        return None, s

# get all drugs with a pharm action, plus the xrefs (from mesh, sparql)
df = get_drug_pa()
df = df.drop_duplicates("mesh")

mesh_qid = wdi_helpers.id_mapper("P486", return_as_set=True, prefer_exact_match=True)
mesh_qid = {k: list(v)[0] if len(v) == 1 else v for k, v in mesh_qid.items()}
unii_qid = wdi_helpers.id_mapper("P652", return_as_set=True, prefer_exact_match=True)
unii_qid = {k: list(v)[0] if len(v) == 1 else v for k, v in unii_qid.items()}
ec_qid = wdi_helpers.id_mapper("P591", return_as_set=True, prefer_exact_match=True)
ec_qid = {k: list(v)[0] if len(v) == 1 else v for k, v in ec_qid.items()}
cas_qid = wdi_helpers.id_mapper("P231", return_as_set=True, prefer_exact_match=True)
cas_qid = {k: list(v)[0] if len(v) == 1 else v for k, v in cas_qid.items()}

df['rn_type'] = df.rn.map(lambda x: infer_type(x)[0])
df['rn'] = df.rn.map(lambda x: infer_type(x)[1])
# see what was missed (nothing except '0')
print("Failed to recognize the following xrefs: {}".format(set(df[df.rn_type.isnull()].rn)))
df.dropna(subset=['rn_type'], inplace=True)

# the ec ones that end in a hyphen are just... wrong
df = df[~df.rn.str.endswith("-")]

df['mesh_qid'] = df.mesh.map(mesh_qid.get)
df['unii_qid'] = df.rn.map(unii_qid.get)
df['ec_qid'] = df.rn.map(ec_qid.get)
df['cas_qid'] = df.rn.map(cas_qid.get)

# looking at each xref type, which have multiple possible qids?
cols = ['unii_qid', 'ec_qid', 'cas_qid']
# need to look at these. they have multiple qids
for col in cols:
    bad_df = df[df[col].map(lambda x: type(x) == set)]
    print("The following df contains drugs where the xref has multiple possible {} QIDs: \n{}".format(col, bad_df))
    df = df[~df.index.isin(bad_df.index)]

# are there cases where the xrefs disagree?
bad_df = df[df[cols].apply(lambda x: len(x.dropna())>1, axis=1)]
if not bad_df.empty:
    print("the following disagree on xrefs: \n{}".format(bad_df))
df = df[~df.index.isin(bad_df.index)]

# take one of the xref qids
df['rn_qid'] = df[cols].apply(lambda x: ','.join(x.dropna()), axis=1)
df.rn_qid = df.rn_qid.replace("", pd.np.nan)
df = df.iloc[:, ~df.columns.isin(cols)]

# check that the mesh_qid and rn_qid are the same if both are found
s = df.dropna(subset=['mesh_qid', 'rn_qid'])
bad_df = s[s.mesh_qid != s.rn_qid]
print("these {} need to be checked (the mesh QID and xref QIDs dont match): \n{}".format(len(bad_df), bad_df))
print("the good news is that there are {} that agree!!!".format(len(s[s.mesh_qid == s.rn_qid])))
df = df[~df.index.isin(bad_df.index)]

# merge the mesh_qid and rn_qid
df['qid'] = df.mesh_qid.combine_first(df.rn_qid)
# and drop any that are still null
print("these {} have no matchable xrefs in wikidata: \n{}".format(len(df[df.qid.isnull()]), df[df.qid.isnull()]))
df.dropna(subset=['qid'], inplace=True)

# are there any that have matched to the same qid?
print("These have matched to the same QID based on xrefs: \n{}".format(df[df.duplicated("qid")]))
df = df[~df.duplicated("qid")]

print("left with {} mesh drugs".format(len(df)))

#################
## Add drug mesh IDs
#################
def make_ref(mesh_id):
    refs = [[
        wdi_core.WDItemID(value=ITEMS['Medical Subject Headings'], prop_nr='P248', is_reference=True),  # stated in mesh
        wdi_core.WDExternalID(value=mesh_id, prop_nr=PROPS['MeSH ID'], is_reference=True),  # mesh id
        wdi_core.WDTime(time=time.strftime('+%Y-%m-%dT00:00:00Z'), prop_nr='P813', is_reference=True)  # retrieved
    ]]
    return refs


records = df.to_dict("records")
records = [r for r in records if r['qid'] and not r['mesh_qid']]
for record in tqdm(records):
    s = [wdi_core.WDString(record['mesh'], PROPS['MeSH ID'], references=make_ref(record['mesh']))]
    item = wdi_core.WDItemEngine(wd_item_id=record['qid'], data=s,
                                 ref_handler=ref_handlers.update_retrieved_if_new_multiple_refs,
                                 fast_run_use_refs=True, fast_run=True, fast_run_base_filter={PROPS['MeSH ID']: ''})
    try:
        item.write(login)
    except Exception as e:
        print(e)
        print(record)
