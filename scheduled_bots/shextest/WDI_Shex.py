from rdflib import Namespace, URIRef
import pprint
import requests
import ../../../wikidataintegrator/wdi_core.py as wdi_core

wdids = []
sparql_query = "PREFIX wdt: <http://www.wikidata.org/prop/direct/>\n\nSELECT ?item WHERE { ?item wdt:P699 ?wpid . } LIMIT 10"
df = wdi_core.WDItemEngine.execute_sparql_query(sparql_query)
for row in df["results"]["bindings"]:
    wdid = row["item"]["value"]
    wdids.append(URIRef(wdid))

WD = Namespace("http://www.wikidata.org/entity/")
schema = requests.get(
    "https://raw.githubusercontent.com/SuLab/Genewiki-ShEx/master/diseases/wikidata-disease-ontology.shex").text

results = dict()
for qid in wdids:
    print(qid)
    results[qid] = wdi_core.WDItemEngine.check_shex_conformance(qid, schema)

pprint.pprint(results)

count = {"passing": 0, "failing": 0}
for result in results.keys():
    if result["result"] == "Passing":
        count["passing"] += 1
    if result["result"] == "Failing":
        count["failing"] += 1

pprint.pprint(count)