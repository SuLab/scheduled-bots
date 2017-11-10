## Need to remove statements with references pointing to old releases
# These didn't get removed because the uniprot ID was dropped, and so weren't even looked at by the interpro bot..
# example: https://www.wikidata.org/w/index.php?title=Q21142273&oldid=570002870
from collections import defaultdict
from wikidataintegrator import wdi_core, wdi_login
from scheduled_bots.local import WDPASS, WDUSER
login = wdi_login.WDLogin(WDUSER, WDPASS)
from tqdm import tqdm

query = """
select ?item ?value where {
  values ?release {wd:Q27877335 wd:Q27135875 wd:Q29947749 wd:Q29564342 wd:Q27818070 wd:Q32846235 wd:Q28543953}
  ?item p:P279 ?s .
  ?s ps:P279 ?value .
  ?s prov:wasDerivedFrom ?ref .
  ?ref pr:P248 ?release .
}"""
# item is the protein, value is the interpo family that is deprecation

query = wdi_core.WDItemEngine.execute_sparql_query(query)
d = {x['item']['value'].split("/")[-1]: x['value']['value'].split("/")[-1] for x in query['results']['bindings']}

#prot_qid = "Q21142273"
#family_qid = "Q24771257"

for prot_qid, family_qid in tqdm(d.items()):
    write=False
    item = wdi_core.WDItemEngine(wd_item_id=prot_qid)
    for s in item.statements:
        if s.get_value() == int(family_qid[1:]):
            setattr(s, 'remove', '')
            write=True
    item.update(item.statements)
    if write:
        try:
            item.write(login, edit_summary="remove outdated family")
        except Exception as e:
            print(prot_qid, e)

#######################
### domains/has part
#######################

query = """
select ?item ?value where {
  values ?release {wd:Q27877335 wd:Q27135875 wd:Q29947749 wd:Q29564342 wd:Q27818070 wd:Q32846235 wd:Q28543953}
  ?item p:P527 ?s .
  ?s ps:P527 ?value .
  ?s prov:wasDerivedFrom ?ref .
  ?ref pr:P248 ?release .
}"""
# item is the protein, value is the interpo family that is deprecation

results = wdi_core.WDItemEngine.execute_sparql_query(query)
d = defaultdict(set)
for x in results['results']['bindings']:
    d[x['item']['value'].split("/")[-1]].add(x['value']['value'].split("/")[-1])
d = {k: set(int(x[1:]) for x in v) for k,v in d.items()}

for prot_qid, family_qid in tqdm(d.items()):
    write=False
    item = wdi_core.WDItemEngine(wd_item_id=prot_qid)
    for s in item.statements:
        if s.get_value() in family_qid:
            setattr(s, 'remove', '')
            write=True
    item.update(item.statements)
    if write:
        try:
            item.write(login, edit_summary="remove outdated domains")
        except Exception as e:
            print(prot_qid, e)

########
## other old style refs
########
query = """
SELECT ?item ?subclass WHERE {
  #?item wdt:P703 wd:Q15978631 .
  ?item wdt:P352 ?uniprot .
  ?item wdt:P527 ?subclass .
  ?subclass wdt:P2926 ?ipr .
  ?subclass p:P2926 ?s .
  ?s prov:wasDerivedFrom ?ref .
  filter not exists {?ref pr:P248 wd:Q41725885}
}"""
results = wdi_core.WDItemEngine.execute_sparql_query(query)
d = defaultdict(set)
for x in results['results']['bindings']:
    d[x['item']['value'].split("/")[-1]].add(x['subclass']['value'].split("/")[-1])
d = {k: set(int(x[1:]) for x in v) for k,v in d.items()}

for prot_qid, family_qid in tqdm(d.items()):
    write=False
    item = wdi_core.WDItemEngine(wd_item_id=prot_qid)
    for s in item.statements:
        if s.get_value() in family_qid:
            setattr(s, 'remove', '')
            write=True
    item.update(item.statements)
    if write:
        try:
            item.write(login, edit_summary="remove outdated domains")
        except Exception as e:
            print(prot_qid, e)

#####
## Other old items to delete, that used the old ref format and I didn't find before
## nov 10 2017 found 19 items. Submitted for deletion
######
query = """SELECT ?item WHERE {
  ?item wdt:P2926 ?ipr .
  ?item p:P2926 ?s .
  ?s prov:wasDerivedFrom ?ref .
  ?ref pr:P348 ?v
}"""
results = wdi_core.WDItemEngine.execute_sparql_query(query)
d = {x['item']['value'].split("/")[-1] for x in results['results']['bindings']}
print("|".join(d))