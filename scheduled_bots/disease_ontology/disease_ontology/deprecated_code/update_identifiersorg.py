from wikidataintegrator import wdi_core, wdi_login
import os
import copy

print("Logging in...")
if "WDUSER" in os.environ and "WDPASS" in os.environ:
    WDUSER = os.environ['WDUSER']
    WDPASS = os.environ['WDPASS']
else:
    raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

login = wdi_login.WDLogin(WDUSER, WDPASS)
query = """
   SELECT * WHERE {
   ?disease wdt:P699 ?doid  .
   FILTER  NOT EXISTS {
     ?disease wdt:P699 ?doid ;
              wdt:P2888 ?exact_match .
     FILTER REGEX(str(?exact_match), "identifiers.org/doid/DOID")
  }
}
"""
missingioiris = wdi_core.WDItemEngine.execute_sparql_query(query)

refStatedIn = wdi_core.WDItemID(value="Q16335166", prop_nr="P248", is_reference=True)
refMiriamID = wdi_core.WDUrl(value="http://www.ebi.ac.uk/miriam/main/collections/MIR:00000233", prop_nr="P854", is_reference=True)

reference = [refStatedIn, refMiriamID]


for do in missingioiris["results"]["bindings"]:
    print(do["disease"]["value"], do["doid"]['value'])
    data = [wdi_core.WDUrl("http://identifiers.org/doid/"+do["doid"]['value'], prop_nr="P2888", references=[copy.deepcopy(reference)])]
    item = wdi_core.WDItemEngine(wd_item_id=do["disease"]["value"].replace("http://www.wikidata.org/entity/", ""), data=data)
    print(item.write(login=login))

