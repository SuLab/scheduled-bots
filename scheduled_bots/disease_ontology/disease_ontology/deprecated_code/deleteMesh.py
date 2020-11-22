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
   ?item p:P486 ?s .
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
  claims = json["claims"]

  for value in claims["P486"]:
      if len(value["qualifiers"]) > 0:
        for reference in value['references']:
          if "P248" in reference['snaks'].keys():
            for stated_in in reference['snaks']['P248']:
              if stated_in['datavalue']["value"]["id"] == "Q5282129":
                value['references'].pop(value['references'].index(reference))
      else:
          for reference in value['references']:
              if "P248" in reference['snaks'].keys():
                  for stated_in in reference['snaks']['P248']:
                      if stated_in['datavalue']["value"]["id"] == "Q5282129":
                          claims["P486"].pop(claims["P486"].index(value))
                          continue

  print(disease.write(login))