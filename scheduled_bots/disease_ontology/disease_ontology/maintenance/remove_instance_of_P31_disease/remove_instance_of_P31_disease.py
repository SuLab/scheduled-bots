from wikidataintegrator import wdi_core, wdi_login
import os

print("Logging in...")
if "WDUSER" in os.environ and "WDPASS" in os.environ:
    WDUSER = os.environ['WDUSER']
    WDPASS = os.environ['WDPASS']
else:
    raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

login = wdi_login.WDLogin(WDUSER, WDPASS)

item = wdi_core.WDItemEngine(wd_item_id="Q1895873")
json = item.get_wd_json_representation()

def remove_disease(qid):
    item = wdi_core.WDItemEngine(wd_item_id=qid)
    json = item.get_wd_json_representation()

    if "P31" in json["claims"].keys():
        for claim in json["claims"]["P31"]:
            print(claim["id"])
            for reference in claim["references"]:
                for snakP248 in reference["snaks"]["P248"]:
                    if snakP248["datavalue"]["value"]["id"] == "Q5282129":
                        return wdi_core.WDItemEngine.delete_statement(statement_id=claim["id"], revision=item.lastrevid, login=login)

results = wdi_core.WDItemEngine.execute_sparql_query("SELECT DISTINCT * WHERE { ?disease p:P31 [ps:P31 wd:Q12136 ; prov:wasDerivedFrom [ pr:P248 wd:Q5282129 ; ]]}")
for result in results["results"]["bindings"]:
    remove_disease(result["disease"]["value"].replace("http://www.wikidata.org/entity/", ""))