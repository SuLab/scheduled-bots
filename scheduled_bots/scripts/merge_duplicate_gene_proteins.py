from tqdm import tqdm

from wikidataintegrator.wdi_core import WDItemEngine, MergeError
from wikidataintegrator.wdi_login import WDLogin
from scheduled_bots.local import WDUSER, WDPASS

login = WDLogin(WDUSER, WDPASS)
s_protein = """
SELECT DISTINCT ?item1 ?item2 ?value {{
	?item1 wdt:P352 ?value .
	?item2 wdt:P352 ?value .
    ?item1 wdt:P31|wdt:P279 wd:Q8054 .
    ?item2 wdt:P31|wdt:P279 wd:Q8054 .
    FILTER NOT EXISTS {{?item1 wdt:P703 wd:Q15978631}}
	FILTER( ?item1 != ?item2 && STR( ?item1 ) < STR( ?item2 ) ) .
}}"""

s_gene = """
SELECT DISTINCT ?item1 ?item2 ?value {{
	?item1 wdt:P351 ?value .
	?item2 wdt:P351 ?value .
    ?item1 wdt:P703 ?taxon1 .
    ?item2 wdt:P703 ?taxon2 .
	FILTER( ?item1 != ?item2 && STR( ?item1 ) < STR( ?item2 ) && ?taxon1 = ?taxon2) .
    FILTER NOT EXISTS {{?item1 wdt:P703 wd:Q15978631}}
}}"""

s = s_gene

items = [{k: v['value'].split("/")[-1] for k, v in x.items()} for x in
         WDItemEngine.execute_sparql_query(s)['results']['bindings']]
for x in tqdm(items):
    try:
        WDItemEngine.merge_items(from_id=x['item2'], to_id=x['item1'], login_obj=login, ignore_conflicts='statement|description|sitelink')
    except MergeError as e:
        print(e)
        pass
