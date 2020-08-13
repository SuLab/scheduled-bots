from wikidataintegrator import wdi_core
import pprint

# get existing combinations:
query_str = """SELECT ?item ?itemLabel (GROUP_CONCAT(?part; separator=";") as ?f) WHERE {
  ?item wdt:P527 ?part .
  ?item wdt:P31 wd:Q1304270 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
} GROUP BY ?item ?itemLabel"""
results = wdi_core.WDItemEngine.execute_sparql_query(query_str)['results']['bindings']
combo_qid = {x['item']['value'].replace("http://www.wikidata.org/entity/", ""): frozenset([y.replace("http://www.wikidata.org/entity/", "") for y in x['f']['value'].split(";")]) for x in results}
qid_combo = {v:k for k,v in combo_qid.items()}
#assert len(combo_qid) == len(qid_combo)

for qid1 in combo_qid.keys():
    for qid2 in combo_qid.keys():
        if qid1 != qid2:
            if combo_qid[qid1] == combo_qid[qid2]:
                print(qid1, combo_qid[qid1], ":", qid2, combo_qid[qid2])
