"""
add rxnorm and ndfrt ids on things
"""

import os
from collections import defaultdict, Counter
from functools import lru_cache
from itertools import chain

import requests
import json

import time
from tqdm import tqdm

from wikidataintegrator import wdi_helpers, wdi_core, wdi_login
from scheduled_bots.local import WDPASS, WDUSER
login = wdi_login.WDLogin(WDUSER, WDPASS)

from scheduled_bots.drugs.pharma.ndfrt import get_roles, get_props

def make_ref(nui):
    refs = [[
        wdi_core.WDItemID(value='Q21008030', prop_nr='P248', is_reference=True),  # stated in ndfrt
        wdi_core.WDExternalID(value=nui, prop_nr='P2115', is_reference=True),  # NDF-RT ID
        wdi_core.WDTime(time=time.strftime('+%Y-%m-%dT00:00:00Z'), prop_nr='P813', is_reference=True)  # retrieved
    ]]
    return refs

rxnorm_qid = wdi_helpers.id_mapper("P3345", return_as_set=True)
print("{} rxnorms have duplicate qids".format(len({k: v for k, v in rxnorm_qid.items() if len(v) > 1})))
rxnorm_qid = {k: list(v)[0] for k, v in rxnorm_qid.items() if len(v) == 1}
nui_qid = wdi_helpers.id_mapper("P2115", return_as_set=True)
print("{} nuis have duplicate qids".format(len({k: v for k, v in nui_qid.items() if len(v) > 1})))
nui_qid = {k: list(v)[0] for k, v in nui_qid.items() if len(v) == 1}
mesh_qid = wdi_helpers.id_mapper("P486", return_as_set=True)

nuis_info = json.load(open("nuis_info.json")) if os.path.exists("nuis_info.json") else dict()
nuis_info = {k: v for k, v in nuis_info.items() if v}
nuis_info = {k: v for k, v in nuis_info.items() if get_roles(v)}

for nui in tqdm(nuis_info):
    s = []
    qid = None
    rxcui = get_props(nuis_info[nui]).get("RxNorm_CUI")
    if nui in nui_qid and rxcui in rxnorm_qid:
        if nui_qid[nui] != rxnorm_qid[rxcui]:
            print("there's something wrong with me!!")
            print(nuis_info[nui]['conceptName'], nui, nui_qid[nui], rxcui, rxnorm_qid[rxcui])
            continue
    if nui in nui_qid and rxcui not in rxnorm_qid:
        # add the rxnorm rxcui onto this qid
        print(nuis_info[nui]['conceptName'], nui, nui_qid[nui], rxcui)
        s = [wdi_core.WDExternalID(rxcui, "P3345", references=make_ref(nui))]
        qid = nui_qid[nui]
    if nui not in nui_qid and rxcui in rxnorm_qid:
        # add the ndfrt nui onto this qid
        print(nuis_info[nui]['conceptName'], nui, rxcui, rxnorm_qid[rxcui])
        s = [wdi_core.WDExternalID(nui, "P2115", references=make_ref(nui))]
        qid = rxnorm_qid[rxcui]
    if s:
        item = wdi_core.WDItemEngine(wd_item_id=qid, data=s, append_value=['P2115','P3345'])
        item.write(login)

for nui in tqdm(nuis_info):
    rxcui = get_props(nuis_info[nui]).get("RxNorm_CUI")
    mesh = get_props(nuis_info[nui]).get("MeSH_DUI")
    if rxcui in rxnorm_qid and mesh and mesh not in mesh_qid:
        print(nui, mesh, rxcui)
        qid = rxnorm_qid[rxcui]
        s = [wdi_core.WDExternalID(mesh, "P486", references=make_ref(nui)),
             wdi_core.WDExternalID(nui, "P2115", references=make_ref(nui))]
        item = wdi_core.WDItemEngine(wd_item_id=qid, data=s, append_value=['P486','P2115','P3345'])
        item.write(login)