from wikidataintegrator import wdi_core
import pandas as pd
from rdflib import Graph
import time
import sys
query = """
SELECT * WHERE {
   ?item wdt:P31 wd:Q7187 .
}
"""
kg = Graph()
results = wdi_core.WDItemEngine.execute_sparql_query(query)
i =0
for qid in results["results"]["bindings"]:
    try:
        # print(qid["item"]["value"].replace("http://www.wikidata.org/entity/", ""))
        kg.parse(qid["item"]["value"]+".ttl")
        i+=1
        print(i)
    except:
        print(print(qid["item"]["value"].replace("http://www.wikidata.org/entity/", "")))
        time.sleep(5)
kg.serialize(destination="genes.ttl", format="turtle")