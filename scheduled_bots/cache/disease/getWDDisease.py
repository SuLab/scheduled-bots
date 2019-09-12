from wikidataintegrator import wdi_core
import pandas as pd
from rdflib import Graph
import time
import sys
query = """
SELECT * WHERE {
   ?item wdt:P31 wd:Q12136  .
}
"""
kg = Graph()
results = wdi_core.WDItemEngine.execute_sparql_query(query)
for qid in results["results"]["bindings"]:
    try:
        # print(qid["item"]["value"].replace("http://www.wikidata.org/entity/", ""))
        kg.parse(qid["item"]["value"]+".ttl")
    except:
        print(print(qid["item"]["value"].replace("http://www.wikidata.org/entity/", "")))
        time.sleep(5)
kg.serialize(destination="diseases.ttl", format="turtle")