"""
One off script to add instance of gene to genes without it

"""
from datetime import datetime
from scheduled_bots.local import WDPASS, WDUSER
from tqdm import tqdm
from wikidataintegrator import wdi_core, wdi_login, wdi_helpers

login = wdi_login.WDLogin(WDUSER, WDPASS)

s = """SELECT ?gene ?entrez WHERE {
  values ?taxids {"559292" "6239" "7227" "7955" "10090" "10116" "9606"}
  ?taxon wdt:P685 ?taxids .
  ?gene wdt:P703 ?taxon .
  ?gene wdt:P351 ?entrez .
  FILTER NOT EXISTS {?gene wdt:P31 wd:Q7187 .}
}"""
r = wdi_core.WDItemEngine.execute_sparql_query(s)['results']['bindings']
qid_entrez = {x['gene']['value'].replace("http://www.wikidata.org/entity/", ""):x['entrez']['value'] for x in r}
print(len(qid_entrez))

def create_ref_statement(entrez, time_str):
    link_to_id = wdi_core.WDString(value=str(entrez), prop_nr='P351', is_reference=True)
    stated_in = wdi_core.WDItemID(value='Q20641742', prop_nr='P248', is_reference=True)
    retrieved = wdi_core.WDTime(time_str, prop_nr='P813', is_reference=True)
    reference = [stated_in, retrieved, link_to_id]
    return reference

for qid, entrez in tqdm(qid_entrez.items()):
    # qid = 'Q18059006'
    # entrez = qid_entrez[qid]
    try:
        item = wdi_core.WDItemEngine(wd_item_id=qid)
    except Exception as e:
        print(qid)
        continue
    j = item.get_wd_json_representation()
    try:
        claim = [x for x in j['claims']['P279'] if x['mainsnak']['datavalue']['value']['id'] == 'Q7187'][0]
        time_str = claim['references'][0]['snaks']['P813'][0]['datavalue']['value']['time']
    except Exception:
        time_str = datetime.now().strftime('+%Y-%m-%dT00:00:00Z')
    ref = create_ref_statement(entrez, time_str)

    data = [wdi_core.WDItemID("Q7187", "P31", references=[ref])]
    item.update(data)
    wdi_helpers.try_write(item, edit_summary="add instance of gene", login=login, record_id=entrez, record_prop="P351")