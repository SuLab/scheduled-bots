from wikidataintegrator import wdi_core, wdi_login
import os

print("Logging in...")
if "WDUSER" in os.environ and "WDPASS" in os.environ:
    WDUSER = os.environ['WDUSER']
    WDPASS = os.environ['WDPASS']
else:
    raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

login = wdi_login.WDLogin(WDUSER, WDPASS)

query = """SELECT DISTINCT ?do_version ?do_versionLabel WHERE {
   ?item ?p ?s .
   ?s prov:wasDerivedFrom ?references .
   ?references pr:P248 ?do_version .
   ?do_version wdt:P629 wd:Q5282129 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}"""
do_versions = []
results = wdi_core.WDItemEngine.execute_sparql_query(query=query)
for result in results["results"]["bindings"]:
  do_versions.append(result["do_version"]["value"].replace("http://www.wikidata.org/entity/", ""))

query = """SELECT DISTINCT ?item WHERE {
   ?item ?p ?s .
   ?s prov:wasDerivedFrom ?references .
   ?references pr:P248 ?do_version .
   ?do_version wdt:P629 wd:Q5282129 .
}"""
list_to_curate = []
results = wdi_core.WDItemEngine.execute_sparql_query(query)
for result in results["results"]["bindings"]:
    list_to_curate.append(result["item"]["value"].replace("http://www.wikidata.org/entity/", ""))


for wdid in list_to_curate:
  disease = wdi_core.WDItemEngine(wd_item_id=wdid, )
  json = disease.wd_json_representation
  claims = json["claims"]
  for prop in claims.keys():
    for value in claims[prop]:
      for reference in value['references']:
        if "P248" in reference['snaks'].keys():
          for stated_in in reference['snaks']['P248']:
            if stated_in['datavalue']["value"]["id"] in do_versions:
              stated_in['datavalue']["value"]["id"] = "Q5282129"
              stated_in['datavalue']["value"]["numeric-id"] = "5282129"

  print(disease.write(login))

