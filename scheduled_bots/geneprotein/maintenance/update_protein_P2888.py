from wikidataintegrator import wdi_core, wdi_login
import copy
import os

print("Logging in...")
if "WDUSER" in os.environ and "WDPASS" in os.environ:
    WDUSER = os.environ['WDUSER']
    WDPASS = os.environ['WDPASS']
else:
    raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")
login = wdi_login.WDLogin(WDUSER, WDPASS)

protein_query = """SELECT * WHERE {
                 ?protein p:P31 ?P31Node ;
                          p:P352 ?P352node .

                 ?P31Node ps:P31 wd:Q8054 ; 
                          prov:wasDerivedFrom ?refernce .
                 ?P352node ps:P352 ?uniprot ;
                       psn:P352 ?uniprot_uri .
                 FILTER NOT EXISTS {?protein wdt:P2888 ?skos .}
                 } LIMIT 10"""
df = wdi_core.WDFunctionsEngine.execute_sparql_query(query=protein_query, as_dataframe=True)



for index, row in df.iterrows():
    qid = row["protein"].replace("http://www.wikidata.org/entity/", "")
    upid = row["uniprot"]

    refStatedIn = wdi_core.WDItemID(value="Q16335163", prop_nr="P248", is_reference=True)
    refUniprot = wdi_core.WDExternalID(value=upid, prop_nr="P352", is_reference=True)
    refURL = wdi_core.WDUrl(value="https://registry.identifiers.org/registry/uniprot", prop_nr="P854",
                            is_reference=True)

    reference = [refStatedIn, refUniprot, refURL]

    statements = []
    statements.append(wdi_core.WDUrl(value="https://identifiers.org/uniprot:" + upid, prop_nr="P2888",
                                     references=[copy.deepcopy(reference)]))
    statements.append(wdi_core.WDUrl(value="http://purl.uniprot.org/uniprot/" + upid, prop_nr="P2888",
                                     references=[copy.deepcopy(reference)]))
    statements.append(wdi_core.WDUrl(value="https://www.ncbi.nlm.nih.gov/protein/" + upid, prop_nr="P2888",
                                     references=[copy.deepcopy(reference)]))
    protein_item = wdi_core.WDItemEngine(wd_item_id=qid, data=statements)
    print(protein_item.write(login))