"""
One off script to remove subclass of gene
"""
from scheduled_bots.local import WDPASS, WDUSER
from tqdm import tqdm
from wikidataintegrator import wdi_core, wdi_login, wdi_helpers

login = wdi_login.WDLogin(WDUSER, WDPASS)

s = """SELECT ?gene ?entrez WHERE {
  values ?taxids {"559292" "6239" "7227" "7955" "10090" "10116" "9606"}
  #values ?taxids {"9606"}
  ?taxon wdt:P685 ?taxids .
  ?gene wdt:P703 ?taxon .
  ?gene wdt:P351 ?entrez .
  ?gene wdt:P279 wd:Q7187 .
}"""
r = wdi_core.WDItemEngine.execute_sparql_query(s)['results']['bindings']
qid_entrez = {x['gene']['value'].replace("http://www.wikidata.org/entity/", ""):x['entrez']['value'] for x in r}
print(len(qid_entrez))

for qid, entrez in tqdm(qid_entrez.items()):
    # qid = 'Q18059006'
    # entrez = qid_entrez[qid]
    item = wdi_core.WDItemEngine(wd_item_id=qid)
    s = wdi_core.WDItemID("Q7187", "P279")
    setattr(s, 'remove', '')
    item.update(data=[s])
    wdi_helpers.try_write(item, edit_summary="remove subclass of gene", login=login, record_id=entrez, record_prop="P351")