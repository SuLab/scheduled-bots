import subprocess
import os
import jsonasobj
import pandas as pd
import requests
from SPARQLWrapper import SPARQLWrapper, JSON
from ShExJSG import ShExC
from pyshex import PrefixLibrary, ShExEvaluator
from sparql_slurper import SlurpyGraph
from wikidataintegrator import wdi_core, wdi_helpers
from datetime import datetime
import json

def run_shex_manifest():
    manifest = jsonasobj.loads(requests.get("https://raw.githubusercontent.com/SuLab/Genewiki-ShEx/master/pathways/reactome/manifest_all.json").text)
    print(os.environ['MANIFEST_URL'])
    f = open("/tmp/disease_shex_report.txt", "w")
    for case in manifest:
        print(case._as_json_dumps())
        if case.data.startswith("Endpoint:"):
            sparql_endpoint = case.data.replace("Endpoint: ", "")
            schema = requests.get(case.schemaURL).text
            shex = ShExC(schema).schema
            print("==== Schema =====")
            #print(shex._as_json_dumps())

            evaluator = ShExEvaluator(schema=shex, debug=True)
            sparql_query = case.queryMap.replace("SPARQL '''", "").replace("'''@START", "")

            df = wdi_core.WDItemEngine.execute_sparql_query(sparql_query)
            for row in df["results"]["bindings"]:
                wdid=row["item"]["value"]
                slurpeddata = SlurpyGraph(sparql_endpoint)
                print(wdid)
                try:
                        # with timeout(120, exception=RuntimeError):
                        results = evaluator.evaluate(rdf=slurpeddata, focus=wdid, debug=False)
                        for result in results:
                            if result.result:
                                print(str(result.focus) + ": CONFORMS")
                                msg = wdi_helpers.format_msg(wdid, wdid, None)
                                wdi_core.WDItemEngine.log("CONFORMS", msg)
                            else:
                                msg = wdi_helpers.format_msg(wdid, wdid, None)
                                wdi_core.WDItemEngine.log("ERROR", s)


                except RuntimeError:
                    print("Continue after 1 minute, no validation happened on"+ wdid)
                    continue
                    #sys.exit()
                    #shapemap = "[{\"node\": \"" + str(result.focus) + "\", \"shape\":\"http://micel.io/genewiki/disease\"}]"
                    #cmd = ["/tmp/shex.js/bin/validate", "-x", "https://raw.githubusercontent.com/SuLab/Genewiki-ShEx/master/diseases/wikidata-disease-ontology.shex", "--endpoint", "https://query.wikidata.org/bigdata/namespace/wdq/sparql", "--map", shapemap]
                    #result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=10)
                    #result = subprocess.run(cmd, stdout=subprocess.PIPE)
                    #ShExErrors = json.loads(result.stdout.decode('utf-8'))
                    # pprint.pprint(ShExErrors)
                    #for error in ShExErrors["errors"]:
                    #    print(error["constraint"]["type"]+": "+error["constraint"]["predicate"])
                    #sys.exit()
                    #print(cm)
    f.close()


__metadata__ = {
    'name': 'PathwayBot',
    'maintainer': 'Andra',
    'tags': ['pathways', 'reactome'],
}
log_dir = "./logs"
run_id = datetime.now().strftime('%Y%m%d_%H:%M')
__metadata__['run_id'] = run_id
log_name = '{}-{}.log'.format(__metadata__['name'], run_id)
if wdi_core.WDItemEngine.logger is not None:
    wdi_core.WDItemEngine.logger.handles = []
wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(__metadata__),
                                    logger_name='reactome')
run_shex_manifest()