"""
1. Remove deprecated statements about diseases that have deprecated DOID statements
2. Un-deprecate the mesh, nci and icd9 statements (I hand checked like a hundred of them and they are all ok, we'll let
the community delete the bad ones)
3. delete the items that are newly empty (no xrefs) (ended up with 135 of them)
"""

import itertools

from scheduled_bots.local import WDPASS, WDUSER
from tqdm import tqdm
from wikidataintegrator import wdi_core, wdi_login, wdi_helpers
import pickle

login = wdi_login.WDLogin(WDUSER, WDPASS)

s = """SELECT ?item ?doid WHERE {
   ?item p:P699 ?s .
   ?s ps:P699 ?doid .
   ?s wikibase:rank wikibase:DeprecatedRank .
}"""

r = wdi_core.WDItemEngine.execute_sparql_query(s)['results']['bindings']
deprecated_diseases = {x['item']['value'].replace("http://www.wikidata.org/entity/", ""):x['doid']['value'] for x in r}
print(len(deprecated_diseases))

# save for posterity, and so we can delete the item that have nothing left on them after we are through
#with open("deprecated_diseases.pkl", 'wb') as f:
#    pickle.dump(deprecated_diseases, f)
#### reload
# deprecated_diseases = pickle.load(open("deprecated_diseases.pkl", 'rb'))

# for each disease, get the other statements we need to delete

for n, (qid, doid) in tqdm(enumerate(deprecated_diseases.items()), total=len(deprecated_diseases)):
    s = """SELECT ?item ?propertyclaim ?b ?value ?pt WHERE {
      ?item p:P699/ps:P699 "{doid}" .
      ?item ?propertyclaim ?id .
      ?property wikibase:claim ?propertyclaim .
      ?property wikibase:propertyType ?pt .
      ?id wikibase:rank wikibase:DeprecatedRank .
      ?id ?b ?value .
      FILTER(regex(str(?b), "http://www.wikidata.org/prop/statement" ))
    }""".replace("{doid}", doid)
    r = wdi_core.WDItemEngine.execute_sparql_query(s)['results']['bindings']

    item = wdi_core.WDItemEngine(wd_item_id=qid)
    data = []
    for x in r:
        value = x['value']['value']
        p = x['propertyclaim']['value'].replace("http://www.wikidata.org/prop/", "")
        pt = x['pt']['value']
        if p in {'P1748'}: # dont remove # NCI Thesaurus ID
            continue
        if pt == "http://wikiba.se/ontology#ExternalId":
            # don't remove any external id but doids
            if p != "P699":
                print(qid, p, value)
                continue
            s = wdi_core.WDExternalID(value, p)
            setattr(s, 'remove', '')
        elif pt == "http://wikiba.se/ontology#WikibaseItem":
            s = wdi_core.WDItemID(value.replace("http://www.wikidata.org/entity/", ""), p)
            setattr(s, 'remove', '')
        elif pt in {"http://wikiba.se/ontology#String", "http://wikiba.se/ontology#Url"}:
            s = wdi_core.WDString(value, p)
            setattr(s, 'remove', '')
        else:
            print(pt)
            continue
        data.append(s)

    item.update(data=data)
    wdi_helpers.try_write(item, edit_summary="remove deprecated statements", login=login, record_id=doid, record_prop="P699")

# undeprecate mesh, icd and nci terms
pids = {'P493', 'P486', 'P1748'}
for pid in pids:
    s = """SELECT ?item ?b ?value ?id WHERE {
      ?item p:{p} ?id .
      ?id wikibase:rank wikibase:DeprecatedRank .
      ?id ?b ?value .
      FILTER(regex(str(?b), "http://www.wikidata.org/prop/statement" ))
    }""".replace("{p}", pid)
    r = wdi_core.WDItemEngine.execute_sparql_query(s)['results']['bindings']
    for record in tqdm(r):
        qid = record['item']['value'].replace("http://www.wikidata.org/entity/", "")
        value = record['value']['value']
        sid = record['id']['value'].replace("http://www.wikidata.org/entity/statement/", "").replace("-", "$", 1).lower()

        item = wdi_core.WDItemEngine(wd_item_id=qid)
        claim = [claim for claim in item.get_wd_json_representation()['claims'][pid] if claim['id'].lower() == sid][0]
        claim['rank'] = 'normal'
        wdi_helpers.try_write(item, edit_summary="undeprecated {}".format(pid), login=login, record_id='', record_prop='')


def grouper(n, iterable):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk

# then find empty items to delete:
# example: Q18975796
deprecated_diseases = list(deprecated_diseases)
all_items = set()
for group in tqdm(grouper(200, deprecated_diseases), total=len(deprecated_diseases)/200):
    qids = " ".join(['wd:' + qid for qid in group])
    s = """
    SELECT ?item (COUNT(*) as ?c) WHERE {
      values ?item {qids}
      ?item ?p ?id .
      ?id wikibase:rank ?rank .
      ?id ?b ?value .
      FILTER(regex(str(?b), "http://www.wikidata.org/prop/statement" ))
    } GROUP BY ?item """.replace("qids", qids)
    r = wdi_core.WDItemEngine.execute_sparql_query(s)['results']['bindings']
    items = [x['item']['value'] for x in r]
    all_items.update(set(items))
all_items = set(x.replace("http://www.wikidata.org/entity/", "") for x in all_items)
items_to_delete = set(deprecated_diseases) - all_items
print(len(items_to_delete))


s = ''
for group in grouper(90, items_to_delete):
    del_template = "{{subst:Rfd group | {xxx} | reason = These diseases have been deprecated and are empty}}\n".replace("{xxx}", '|'.join(group))
    s+=del_template

with open("delete_template",'w') as f:
    f.write(s)