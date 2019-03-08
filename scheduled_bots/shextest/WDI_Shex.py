from rdflib import Namespace, URIRef
import pprint
import requests
from wikidataintegrator import wdi_core
import signal
import time

class TimeoutException(Exception):   # Custom exception class
    pass

def timeout_handler(signum, frame):   # Custom signal handler
    raise TimeoutException

# Change the behavior of SIGALRM
signal.signal(signal.SIGALRM, timeout_handler)

wdids = []
sparql_query = "PREFIX wdt: <http://www.wikidata.org/prop/direct/>\n\nSELECT ?item WHERE { ?item wdt:P699 ?wpid . }"
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
    signal.alarm(120)
    try:
        results[qid] = wdi_core.WDItemEngine.check_shex_conformance(qid, schema, output="all")
    except TimeoutException:
        print("timeout")
        continue  # continue the for loop if function A takes more than 120 seconds
    except ValueError:
        print("SPARQL endpoint does not return values")
        time.sleep(60)
        continue
    else:
        # Reset the alarm
        signal.alarm(0)

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