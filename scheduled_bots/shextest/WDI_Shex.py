from rdflib import Namespace, URIRef
import pprint
import requests
from wikidataintegrator import wdi_core

"""
def check_shex_conformance(qid, schema, endpoint="https://query.wikidata.org/sparql", debug=False):
    results = dict()
    results["wdid"] = qid
    slurpeddata = SlurpyGraph(endpoint)
    for p, o in slurpeddata.predicate_objects(qid):
        # for a, b in slurpeddata.predicate_objects(o):
        pass
    for result in pyshex.ShExEvaluator(rdf=slurpeddata, schema=schema, focus=qid).evaluate():
        shex_result = dict()
        if result.result:
            shex_result["result"] = "Passing"
        else:
            shex_result["result"] = "Failing"
            print("reason: " + result.reason)
        shex_result["reason"] = result.reason

    return shex_result
"""

wdids = []
sparql_query = "PREFIX wdt: <http://www.wikidata.org/prop/direct/>\n\nSELECT ?item WHERE { ?item wdt:P699 ?wpid . } LIMIT 100"
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
    results[qid] = wdi_core.WDItemEngine.check_shex_conformance(qid, schema, output="all")

# pprint.pprint(results)
errors = dict()
for qid in results.keys():
    if not results[qid]["result"]:
        print(results[qid]["reason"])
        errors[qid] = results[qid]["reason"]

pprint.pprint(errors)

count = {"passing": 0, "failing": 0}
for result in results.keys():
    if results[result]["result"]:
        count["passing"] += 1
    if not results[result]["result"]:
        count["failing"] += 1

pprint.pprint(count)