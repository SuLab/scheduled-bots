from wikidataintegrator import wdi_core, wdi_login, wdi_helpers
from wikidataintegrator.wdi_helpers import try_write
import os
from rdflib import Graph, URIRef
import pandas as pd
import copy
from datetime import datetime
import traceback

print("Logging in...")
if "WDUSER" in os.environ and "WDPASS" in os.environ:
    WDUSER = os.environ['WDUSER']
    WDPASS = os.environ['WDPASS']
else:
    raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

login = wdi_login.WDLogin(WDUSER, WDPASS)

def createDOReference(doid):
    statedin = wdi_core.WDItemID("Q5282129", prop_nr="P248", is_reference=True)
    retrieved = datetime.now()
    timeStringNow = retrieved.strftime("+%Y-%m-%dT00:00:00Z")
    refRetrieved = wdi_core.WDTime(timeStringNow, prop_nr="P813", is_reference=True)
    doid = wdi_core.WDExternalID(doid, prop_nr="P699", is_reference=True)
    return [statedin, refRetrieved, doid]

def createIORef():
    statedin = wdi_core.WDItemID("Q16335166", prop_nr="P248", is_reference=True)
    referenceURL = wdi_core.WDUrl("https://registry.identifiers.org/registry/doid", prop_nr="P854", is_reference=True)
    return [statedin, referenceURL]

def create(doid):
    global soQids
    do_reference = createDOReference(doid)
    identorg_reference = createIORef()
    tuple = df_doNative[df_doNative["doid"]==doid]
    dorow = tuple.iloc[0]
    statements = []
    # Disease Ontology ID (P31)
    statements.append(wdi_core.WDString(value=dorow["doid"], prop_nr="P699", references=[copy.deepcopy(do_reference)]))
    # exact match (P2888)
    statements.append(wdi_core.WDUrl(value=dorow["do_uri"], prop_nr="P2888", references=[copy.deepcopy(do_reference)]))
    # identifiers.org URI
    statements.append(wdi_core.WDUrl("http://identifiers.org/doid/"+dorow["doid"], prop_nr="P2888", references=[copy.deepcopy(identorg_reference)]))
    uri = str(dorow["do_uri"])

    query = """PREFIX obo: <http://www.geneontology.org/formats/oboInOwl#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>

            SELECT * WHERE {"""
    query += "<" + uri + "> rdfs:subClassOf [ owl:onProperty doid:has_symptom ; owl:someValuesFrom ?symptom ] .} "

    for row in doGraph.query(query):
        if str(row[0]) not in soQids.keys():
            continue
        statements.append(wdi_core.WDItemID(value=soQids[str(row[0])].replace("http://www.wikidata.org/entity/", ""),
                                          prop_nr="P780", references=[copy.deepcopy(do_reference)]))
    query="""PREFIX obo: <http://www.geneontology.org/formats/oboInOwl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>

        SELECT ?subClassOf WHERE {"""
    query+= "<"+uri+"> rdfs:subClassOf ?subClassOf .  FILTER (REGEX(str(?subClassOf), 'http', 'i'))} "

    for row in doGraph.query(query):
        if row[0].replace("http://purl.obolibrary.org/obo/DOID_", "DOID:") not in doQids.keys():
            doQids[row[0].replace("http://purl.obolibrary.org/obo/DOID_", "DOID:")] = create(row[0].replace("http://purl.obolibrary.org/obo/DOID_", "DOID:"))
        statements.append(wdi_core.WDItemID(value=doQids[row[0].replace("http://purl.obolibrary.org/obo/DOID_", "DOID:")].replace("http://www.wikidata.org/entity/", ""),
                                           prop_nr="P279", references=[copy.deepcopy(do_reference)]))
    query="""PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

        SELECT ?exactMatch WHERE {"""
    query+= "<"+uri+"> skos:exactMatch ?exactMatch .}"
    for row in doGraph.query(query):
        extID = row[0]
        if "MESH:" in extID:
            statements.append(wdi_core.WDExternalID(row["exactMatch"].replace("MESH:", ""), prop_nr="P486", references=[copy.deepcopy(do_reference)]))
        if "NCI:" in extID:
            statements.append(wdi_core.WDExternalID(row["exactMatch"], prop_nr="P1748", references=[copy.deepcopy(do_reference)]))
        if "ICD10CM:" in extID:
            statements.append(wdi_core.WDExternalID(row["exactMatch"], prop_nr="P4229", references=[copy.deepcopy(do_reference)]))
        if "UMLS_CUI:" in extID:
            statements.append(wdi_core.WDExternalID(row["exactMatch"], prop_nr="P2892", references=[copy.deepcopy(do_reference)]))

    item = wdi_core.WDItemEngine(wd_item_id=doQids[doid].replace("http://www.wikidata.org/entity/", ""), data=statements, keep_good_ref_statements=True)

    if item.get_label() == "":
        item.set_label(dorow["label"], lang="en")
        if item.get_description() == "":
            item.set_description("human disease", lang="en")
    elif item.get_label() != dorow["label"]:
        aliases = item.get_aliases()
        if dorow["label"] not in aliases:
            aliases.append(dorow["label"])
            item.set_aliases(aliases)

    item.get_wd_json_representation()
    try_write(item, record_id=doid, record_prop="P699", edit_summary="Updated a Disease Ontology term",
              login=login)


try:
    print("\nDownloading the Disease Ontology...")
    url = "https://raw.githubusercontent.com/DiseaseOntology/HumanDiseaseOntology/main/src/ontology/releases/2020-11-11/doid.owl"

    doGraph = Graph()
    doGraph.parse(url, format="xml")

    df_doNative = pd.DataFrame(columns=["do_uri", "doid", "label"])

    qres = doGraph.query(
        """
           PREFIX obo: <http://www.geneontology.org/formats/oboInOwl#>
           PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
           PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>
    
           SELECT DISTINCT ?do_uri ?doid ?label 
           WHERE {
              ?do_uri obo:id ?doid ;
                      rdfs:label ?label .
    
           } """)

    for row in qres:
        df_doNative = df_doNative.append({
         "do_uri": str(row[0]),
         "doid": str(row[1]),
         "label":  str(row[2]),
          }, ignore_index=True)

    query = """
      SELECT DISTINCT ?disease ?doid WHERE {?disease  wdt:P699 ?doid .}
    """
    df_wd = wdi_core.WDFunctionsEngine.execute_sparql_query(query, as_dataframe=True)

    doQids = {}
    inwikidata=wdi_core.WDFunctionsEngine.execute_sparql_query(query, as_dataframe=True)
    for index, row in inwikidata.iterrows():
        doQids[row["doid"]] = row["disease"]

    QidsDo = dict()
    for key in doQids.keys():
        QidsDo[doQids[key]] = key

    soQids = {}
    query = """
    SELECT ?symptom ?symptomLabel ?soid WHERE {
       ?symptom wdt:P8656 ?soid .
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
    }
    """
    inwikidata = wdi_core.WDFunctionsEngine.execute_sparql_query(query, as_dataframe=True)
    for index, sorow in inwikidata.iterrows():
        soQids["http://purl.obolibrary.org/obo/SYMP_" + sorow["soid"]] = sorow["symptom"]

    for index, row in df_doNative.iterrows():
        doid = row["doid"]
        create(doid)
except Exception as e:
    traceback.print_exc()
    wdi_core.WDItemEngine.log("ERROR", wdi_helpers.format_msg("Foutje", "P699", None, str(e), type(e)))

