from wikidataintegrator import wdi_core, wdi_login
import os

print("Logging in...")
if "WDUSER" in os.environ and "WDPASS" in os.environ:
    WDUSER = os.environ['WDUSER']
    WDPASS = os.environ['WDPASS']
else:
    raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

login = wdi_login.WDLogin(WDUSER, WDPASS)

query = """SELECT DISTINCT ?item ?itemLabel ?mappingrelationLabel WHERE {
   ?item p:P1748 ?s .
   ?s pq:P4390 ?mappingrelation .
   ?s prov:wasDerivedFrom ?references .
   ?references pr:P248 wd:Q5282129 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
"""
list_to_curate = []
results = wdi_core.WDItemEngine.execute_sparql_query(query)
for result in results["results"]["bindings"]:
    list_to_curate.append(result["item"]["value"].replace("http://www.wikidata.org/entity/", ""))

for wdid in list_to_curate:
  disease = wdi_core.WDItemEngine(wd_item_id=wdid)
  json = disease.wd_json_representation
  claims = json["claims"]["P1748"]
  for value in claims:
    for reference in value['references']:
      if "P248" in reference['snaks'].keys():
        for stated_in in reference['snaks']['P248']:
          if stated_in['datavalue']["value"]["id"] == "Q5282129":
            if len(value["qualifiers"].keys()) >0:
              # pprint.pprint(json)
              wdi_core.WDItemEngine.delete_statement(value["id"],disease.lastrevid, login)
              continue