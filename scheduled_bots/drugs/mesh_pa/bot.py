# bot to add mesh pharmacologic action mesh terms and their subclass hieracrchy to wikidata
import pandas as pd

pd.set_option("display.width", 200)
import time
import re

from tqdm import tqdm

from scheduled_bots.drugs.mesh_pa.dumper import get_mesh_tree, get_mesh_pa_counts, get_drug_pa
from wikidataintegrator import wdi_helpers, wdi_core, wdi_login, ref_handlers
from scheduled_bots.local import WDPASS, WDUSER
PROPS = {'MeSH ID': 'P486',
         'MeSH Code': 'P672',
         'subclass': 'P279',
         'has role': 'P2868'}
ITEMS = {'Medical Subject Headings': 'Q199897'}
login = wdi_login.WDLogin(WDUSER, WDPASS)


def make_ref(mesh_id):
    refs = [[
        wdi_core.WDItemID(value=ITEMS['Medical Subject Headings'], prop_nr='P248', is_reference=True),  # stated in mesh
        wdi_core.WDExternalID(value=mesh_id, prop_nr=PROPS['MeSH ID'], is_reference=True),  # mesh id
        wdi_core.WDTime(time=time.strftime('+%Y-%m-%dT00:00:00Z'), prop_nr='P813', is_reference=True)  # retrieved
    ]]
    return refs

mesh_qid = wdi_helpers.id_mapper("P486", return_as_set=True, prefer_exact_match=True)
mesh_qid = {k: list(v)[0] if len(v) == 1 else v for k, v in mesh_qid.items()}

#################
## create all mesh drug and pharm action data from mesh (sparql endpoint)
#################

# get mesh PA terms and tree nums
df = get_mesh_tree()

# get PA use counts
df_drug_pa = get_mesh_pa_counts()

# merge these two dfs together
df = df.merge(df_drug_pa, how="outer", on="mesh")
df.c = df.c.fillna(0)
df['meshLabel'] = df.meshLabel_x.combine_first(df.meshLabel_y)
del df['meshLabel_x']
del df['meshLabel_y']
# there's 5 mesh terms that are in a different tree. Skip!
df.dropna(subset=["treeNum"], inplace=True)

# get the mesh PA -> QID mappings
df['qid'] = df.mesh.map(mesh_qid.get)
# make sure we have 1-to-1 mesh-qid mappings
baddies = df[df.qid.map(lambda x: type(x) == set)]
if not baddies.empty:
    print("These pharm actions have dupes QIDs: {}".format(baddies))
df = df[df.qid.map(lambda x: type(x) != set)]

# first I want to make sure the top level terms are in Wikidata. If not, throw an error
# (some terms won't be in wikidata, and we'll create them, if they are actually used)
tl2 = df[df.treeNum.isin(df.topLevel2)]
assert tl2.qid.notnull().all()

# create list of records for creating/updating all PA terms
agg = {k: lambda x: list(x)[0] for k in {'note', 'qid', 'aliases', 'meshLabel', 'mesh'}}
agg['treeNum'] = lambda x: set(x)
records = df.groupby("mesh").agg(agg).to_dict("records")
"""
{'aliases': 'Abortifacients|Contraceptive Agents, Postconception',
 'mesh': 'D000019',
 'meshLabel': 'Abortifacient Agents',
 'note': 'Chemical substances that interrupt pregnancy after implantation.',
 'qid': 'Q50414775',
 'treeNum': {'D27.505.696.875.131', 'D27.505.954.705.131'}}
"""
#################
## create all PA items
#################

for r in tqdm(records):
    s = [wdi_core.WDString(r['mesh'], PROPS['MeSH ID'], references=make_ref(r['mesh']))]
    s.extend([wdi_core.WDString(tree, PROPS['MeSH Code'], references=make_ref(r['mesh'])) for tree in r['treeNum']])

    if r['qid']:
        item = wdi_core.WDItemEngine(wd_item_id=r['qid'], data=s,
                                     ref_handler=ref_handlers.update_retrieved_if_new_multiple_refs,
                                     fast_run_use_refs=True, fast_run=True, fast_run_base_filter={PROPS['MeSH ID']: ''})
    else:
        item = wdi_core.WDItemEngine(data=s, item_name="foo", domain="bar",
                                     ref_handler=ref_handlers.update_retrieved_if_new_multiple_refs,
                                     fast_run_use_refs=True, fast_run=True, fast_run_base_filter={PROPS['MeSH ID']: ''})
    if not item.get_label():
        item.set_label(r['meshLabel'])
    if item.get_description() == "":
        desc = '. '.join(i.capitalize() for i in r['note'][:249].split('. '))
        desc = desc[:-1] if desc.endswith('.') else desc
        item.set_description(desc)
    if r['aliases']:
        item.set_aliases(set(r['aliases'].split("|")), append=True)
    if item.require_write:
        print(item.wd_item_id)
    item.write(login)
    r['qid'] = item.wd_item_id

#################
## create all PA items subclass links
#################
for r in tqdm(records):
    parent_trees = [x.rsplit(".", 1)[0] for x in r['treeNum']]
    parent_records = [x for x in records if any(y in x['treeNum'] for y in parent_trees)]
    if not r['qid']:
        print(r)
        continue
    if any(x['qid'] is None for x in parent_records):
        print(r)
        continue
    s = [wdi_core.WDItemID(parent_record['qid'], PROPS['subclass'], references=make_ref(r['mesh'])) for parent_record in
         parent_records]
    item = wdi_core.WDItemEngine(wd_item_id=r['qid'], data=s,
                                 ref_handler=ref_handlers.update_retrieved_if_new_multiple_refs,
                                 fast_run_use_refs=True, fast_run=True, fast_run_base_filter={PROPS['MeSH ID']: ''},
                                 append_value=[PROPS['subclass']])
    item.write(login)

#################
## Add drug PAs!!!
#################

df = get_drug_pa()

df['pa_qid'] = df.pa.map(mesh_qid.get)
df['qid'] = df.mesh.map(mesh_qid.get)
df.dropna(subset=['pa_qid'], inplace=True)
df = df[df.pa_qid.map(lambda x: pd.notnull(x) and type(x) == str)]
s = df.groupby("qid").agg({'pa_qid': lambda x: list(x)}).reset_index()
records = df.merge(s, how="inner", on="qid")[['mesh', 'qid', 'pa_qid_y']].drop_duplicates("mesh").to_dict("records")
for r in tqdm(records):
    s = [wdi_core.WDItemID(x, PROPS['has role'], references=make_ref(r['mesh'])) for x in r['pa_qid_y']]
    item = wdi_core.WDItemEngine(wd_item_id=r['qid'], data=s, append_value=[PROPS['has role']],
                                 ref_handler=ref_handlers.update_retrieved_if_new_multiple_refs,
                                 fast_run_use_refs=True, fast_run=True, fast_run_base_filter={PROPS['MeSH ID']: ''})
    try:
        item.write(login)
    except Exception as e:
        print(e)
        print(r)