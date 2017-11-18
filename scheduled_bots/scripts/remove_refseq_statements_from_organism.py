from wikidataintegrator import wdi_core, wdi_login
from scheduled_bots.local import WDPASS, WDUSER

login = wdi_login.WDLogin(WDUSER, WDPASS)
from tqdm import tqdm

query = """
select ?itemLabel ?item ?rf ?tax  where {
  ?item wdt:P2249 ?rf .
  ?item wdt:P685 ?tax .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}"""

query = wdi_core.WDItemEngine.execute_sparql_query(query)
d = {x['item']['value'].split("/")[-1]: x['rf']['value'].split("/")[-1] for x in query['results']['bindings']}

for item_qid, refseq in tqdm(d.items()):
    write = False
    item = wdi_core.WDItemEngine(wd_item_id=item_qid)
    for s in item.statements:
        if s.get_value() == refseq:
            setattr(s, 'remove', '')
            write = True
    item.update(item.statements)
    if write:
        try:
            item.write(login, edit_summary="remove refseq genome id, moving to chromosome item")
        except Exception as e:
            print(item_qid, e)
