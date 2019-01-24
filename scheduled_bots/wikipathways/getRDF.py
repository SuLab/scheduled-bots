import requests
import io
import zipfile
from contextlib import closing
from rdflib import Graph
from bs4 import BeautifulSoup, SoupStrainer
import pprint
from wikidataintegrator import wdi_core, wdi_login, wdi_property_store
from SPARQLWrapper import SPARQLWrapper, JSON
from time import gmtime, strftime
import copy
import os
import traceback
import sys


## First the data from Wikipathways is acquired. The data is regulary update at http://data.wikipathways.org
## After loading the data is loaded in a rdflib graph, which will maintain in memory.

url = 'http://data.wikipathways.org/current/rdf'
page = requests.get(url).text
files = []
for link in BeautifulSoup(page , "lxml", parse_only=SoupStrainer('a')):
    address = str(link).split("\"")
    if len(address)>1:
        filename = address[1].replace("./", "/")
        if len(filename) >1:
            if filename not in files:
                if filename != "./":
                    files.append(url+filename)
temp = Graph()
wdi_property_store.wd_properties['P2410'] = {
        'datatype': 'string',
        'name': 'Wikipathways ID',
        'domain': ['pathways'],
        'core_id': True
    }

fast_run_base_filter = {'P2410': ''}

wpids = []
for file in set(files):
    if "rdf-wp" in file:  # get the most accurate file
        print(file)
        u = requests.get(file)
        with closing(u), zipfile.ZipFile(io.BytesIO(u.content)) as archive:
            for member in archive.infolist():
                nt_content = archive.read(member)
                # print(nt_content)
                temp.parse(data=nt_content.decode(), format="turtle")


        wp_query = """prefix dcterm: <http://purl.org/dc/terms/>
        prefix wp: <http://vocabularies.wikipathways.org/wp#>
        SELECT DISTINCT ?wpid WHERE {
          ?s rdf:type <http://vocabularies.wikipathways.org/wp#Pathway> ;
             dcterm:identifier ?wpid ;
             ?p <http://vocabularies.wikipathways.org/wp#Curation:AnalysisCollection> ;
             wp:organism <http://purl.obolibrary.org/obo/NCBITaxon_9606> .
          }
                   """

        qres = temp.query(wp_query)
        for row in qres:
            print("%s" % row)
            wpids.append(str(row[0]))

pprint.pprint(wpids)

## Once loaded memory it is ready for querying an subsequent pushing to Wikidata.

# Login Wikidata
logincreds = wdi_login.WDLogin(user=os.environ["wd_user"], pwd=os.environ["pwd"])
wikidata_sparql = SPARQLWrapper("https://query.wikidata.org/bigdata/namespace/wdq/sparql")

def get_PathwayElements(pathway="", datatype="GeneProduct"):
    query =  """PREFIX wp:      <http://vocabularies.wikipathways.org/wp#>
           PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#>
           PREFIX dcterms: <http://purl.org/dc/terms/>

           select distinct ?pathway (str(?label) as ?geneProduct) ?id where {
            ?metabolite a wp:"""
    query += datatype
    query += """  ;
                    rdfs:label ?label ;"""
    if datatype == "Metabolite":
        query += "   wp:bdbPubChem ?id ;"
    if datatype == "GeneProduct":
        query += "   wp:bdbEntrezGene ?id ;"
    query += """
                    dcterms:isPartOf ?pathway .
            ?pathway a wp:Pathway ;
                   dcterms:identifier
            """
    query += "\"" + pathway +"\"^^xsd:string .}"
    qres2 = temp.query(query)

    ids = []
    for row in qres2:
        ids.append("\"" + str(row[2]).replace("http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID", "").replace("http://identifiers.org/ncbigene/", "") + "\"")

    # Check for existence of the ids in wikidata
    wd_query = "SELECT DISTINCT * WHERE {VALUES ?id {"
    wd_query += " ".join(ids)
    if datatype == "Metabolite":
        wd_query += "} ?item wdt:P662 ?id . }"
    if datatype == "GeneProduct":
        wd_query += "} ?item wdt:P351 ?id . }"


    wikidata_sparql.setQuery(wd_query)
    #print(wd_query)

    wikidata_sparql.setReturnFormat(JSON)
    results = wikidata_sparql.query().convert()
    for result in results["results"]["bindings"]:
        if "P527" not in prep.keys():
            prep["P527"] = []
        prep["P527"].append(wdi_core.WDItemID(result["item"]["value"].replace("http://www.wikidata.org/entity/", ""), prop_nr='P527', references=[copy.deepcopy(wikipathways_reference)]))

for pwid in wpids:
  try:
    prep = dict()
    # Defining references
    refStatedIn = wdi_core.WDItemID(value="Q7999828", prop_nr='P248', is_reference=True)
    timeStringNow = strftime("+%Y-%m-%dT00:00:00Z", gmtime())
    refRetrieved = wdi_core.WDTime(timeStringNow, prop_nr='P813', is_reference=True)
    refWikiPathways = wdi_core.WDString(pwid, prop_nr='P2410', is_reference=True)
    wikipathways_reference = [refStatedIn, refRetrieved, refWikiPathways]

    get_PathwayElements(pathway=pwid,datatype="Metabolite")
    get_PathwayElements(pathway=pwid, datatype="GeneProduct")



    # P703 = found in taxon, Q15978631 = "Homo sapiens"
    prep["P703"] = [wdi_core.WDItemID(value="Q15978631", prop_nr='P703', references=[copy.deepcopy(wikipathways_reference)])]


    query = """
        PREFIX wp:    <http://vocabularies.wikipathways.org/wp#>
        PREFIX gpml:    <http://vocabularies.wikipathways.org/gpml#>
        PREFIX dcterms: <http://purl.org/dc/terms/>
    SELECT DISTINCT ?pathway ?pwId ?pwLabel
    WHERE {
       VALUES ?pwId {"""
    query += "\""+pwid+"\"^^xsd:string}"
    query += """
       ?pathway a wp:Pathway ;
                dc:title ?pwLabel ;
                dcterms:identifier ?pwId ;
                <http://vocabularies.wikipathways.org/wp#isAbout> ?details ;
                wp:organismName "Homo sapiens"^^xsd:string .
    }"""
    #print(query)
    qres3 = temp.query(query)

    for row in qres3:
        print(row[1])
        print(str(row[2]))
        # P31 = instance of
        prep["P31"] = [wdi_core.WDItemID(value="Q4915012",prop_nr="P31", references=[copy.deepcopy(wikipathways_reference)])]

        # P2410 = WikiPathways ID
        prep["P2410"] = [wdi_core.WDString(pwid, prop_nr='P2410', references=[copy.deepcopy(wikipathways_reference)])]

        # P2888 = exact match
        prep["P2888"] = [wdi_core.WDUrl("http://identifiers.org/wikipathways/"+str(row[1]), prop_nr='P2888', references=[copy.deepcopy(wikipathways_reference)])]

        query = """
        PREFIX wp:    <http://vocabularies.wikipathways.org/wp#>
        PREFIX dcterms: <http://purl.org/dc/terms/>
        select ?pubmed

        WHERE {
         ?pubmed  a       wp:PublicationReference ;
                dcterms:isPartOf <"""

        query+= str(row[0])
        query += """> .}

        """
        qres4 = temp.query(query)


        pubmed_citations = []
        for pubmed_result in qres4:
            pubmed_citations.append("\""+str(row[0]).replace("http://identifiers.org/pubmed/", "")+"\"")

        query = "SELECT * WHERE { VALUES ?pmid {"
        query += " ".join(pubmed_citations)
        query += "} ?item wdt:P698 ?pmid .}"
        # print(query)
        wikidata_sparql.setQuery(query)
        wikidata_results = wikidata_sparql.query().convert()
        for wikidata_result in wikidata_results["results"]["bindings"]:
            # P2860 = cites
            if 'P2860' not in prep.keys():
                prep["P2860"] = []
            prep['P2860'].append(wdi_core.WDItemID(value=wikidata_result["item"]["value"].replace("http://www.wikidata.org/entity/", ""), prop_nr='P2860',
                                               references=[copy.deepcopy(wikipathways_reference)]))

        #pprint.pprint(prep)
        data2add = []
        for key in prep.keys():
            for statement in prep[key]:
                data2add.append(statement)
                print(statement.prop_nr, statement.value)
        wdPage = wdi_core.WDItemEngine(data=data2add, server="www.wikidata.org",
                                        fast_run_base_filter=fast_run_base_filter, fast_run_use_refs=True)

        wdPage.set_label(str(row[2]), lang="en")
        wdPage.set_description("biological pathway in human", lang="en")

       #wd_json_representation = wdPage.get_wd_json_representation()

        #pprint.pprint(wd_json_representation)
        print(wdPage.write(logincreds))
  except Exception as e:
      print(traceback.format_exc())
      wdi_core.WDItemEngine.log('ERROR', '{main_data_id}, "{exception_type}", "{message}", {wd_id}, {duration}'.format(
          main_data_id=pwid,
          exception_type=type(e),
          message=e.__str__(),
          wd_id='-',
          duration=''))






