from scheduled_bots.ontology.DOID_obographs_bot import DOGraph
from tqdm import tqdm
from wikidataintegrator import wdi_core, wdi_login, wdi_helpers
from scheduled_bots.local import WDUSER, WDPASS
login = wdi_login.WDLogin(WDUSER, WDPASS)
import json
json_path='doid.json'
with open(json_path) as f:
    d = json.load(f)
graphs = {g['id']: g for g in d['graphs']}
graph = graphs['http://purl.obolibrary.org/obo/doid.owl']
do = DOGraph(graph, login, False)
nodes = sorted(do.nodes.values(), key=lambda x: x.doid)

doid_qid = wdi_helpers.id_mapper("P699")

for node in tqdm(nodes):
    if node.doid not in doid_qid:
        continue
    # remove (or check to make sure dont exist) statements on ICD10 that should really be ICD10CM
    icd10s = [x.split(":",1)[1] for x in node.xrefs if x.startswith("ICD10CM")]
    icd9s = [x.split(":",1)[1] for x in node.xrefs if x.startswith("ICD9CM")]
    ss = []
    for icd10 in icd10s:
        s = wdi_core.WDExternalID(icd10, "P494")
        setattr(s, "remove", "")
        ss.append(s)
    for icd9 in icd9s:
        s = wdi_core.WDExternalID(icd9, "P493")
        setattr(s, "remove", "")
        ss.append(s)
    item = wdi_core.WDItemEngine(wd_item_id=doid_qid[node.doid], data=ss)

    wdi_helpers.try_write(item, node.doid, record_prop="P699", login=login, edit_summary="remove incorrect ICD10/ICD9")