import json
import argparse
import copy
import traceback
from datetime import datetime
import os
from rdflib import Graph
import zipfile
import io
from contextlib import closing
from bs4 import BeautifulSoup, SoupStrainer
from SPARQLWrapper import SPARQLWrapper, JSON
import pprint


import requests
# from scheduled_bots.wikipathways import CHROMOSOME, IGNORE_SYNONYMS, DrugCombo, EVIDENCE_LEVEL, TRUST_RATING
from wikidataintegrator import wdi_core, wdi_login, wdi_helpers
from wikidataintegrator.ref_handlers import update_retrieved_if_new_multiple_refs
from wikidataintegrator.wdi_helpers import try_write

from scheduled_bots import PROPS, ITEMS, get_default_core_props

CACHE_SIZE = 10000
CACHE_TIMEOUT_SEC = 300  # 5 min

try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

PROPS = {
    'Wikipathways ID': 'P2410',
    'instance of': 'P31',
    'stated in': 'P248',
    'reference URL': 'P854',
    'Entrez Gene ID ': 'P351',
    'found in taxon': 'P703',
    'PubMed ID': 'P698',
    'curator': 'P1640',
    'retrieved': 'P813'
}

ITEMS = {
    'Wikipathways': 'Q7999828',
    'Homo sapiens': 'Q15978631'
}

core_props = get_default_core_props()
#core_props.update({PROPS['WikiPathways ID']})

__metadata__ = {
    'name': 'PathwayBot',
    'maintainer': 'Andra',
    'tags': ['pathways'],
    'properties': list(PROPS.values())
}

fast_run_base_filter = {'P2410': ''}

def create_reference(pathway_id, retrieved):
    refStatedIn = wdi_core.WDItemID(value=ITEMS['Wikipathways'], prop_nr=PROPS['stated in'], is_reference=True)
    timeStringNow = retrieved.strftime("+%Y-%m-%dT00:00:00Z")
    refRetrieved = wdi_core.WDTime(timeStringNow, prop_nr=PROPS['retrieved'], is_reference=True)
    refWikiPathwaysID = wdi_core.WDString(value=pathway_id, prop_nr=PROPS['Wikipathways ID'], is_reference=True)

    pathway_reference = [refStatedIn, refRetrieved, refWikiPathwaysID]
    return pathway_reference

def panic(pathway_id, msg='', msg_type=''):
    s = wdi_helpers.format_msg(pathway_id, PROPS['Wikipathways ID'], None, msg, msg_type)
    wdi_core.WDItemEngine.log("ERROR", s)
    print(s)
    return None

def main(retrieved, fast_run, write):
    login = wdi_login.WDLogin(WDUSER, WDPASS)
    temp = Graph()
    url = 'http://data.wikipathways.org/current/rdf'
    page = requests.get(url).text
    files = []
    for link in BeautifulSoup(page, "lxml", parse_only=SoupStrainer('a')):
        address = str(link).split("\"")
        if len(address) > 1:
            filename = address[1].replace("./", "/")
            if len(filename) > 1:
                if filename not in files:
                    if filename != "./":
                        files.append(url + filename)
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
            print("size: "+str(len(temp)))

    wp_query = """prefix dcterm: <http://purl.org/dc/terms/>
            prefix wp: <http://vocabularies.wikipathways.org/wp#>
            SELECT DISTINCT ?wpid WHERE {
              ?s rdf:type <http://vocabularies.wikipathways.org/wp#Pathway> ;
                 dcterm:identifier ?wpid ;
                 ?p <http://vocabularies.wikipathways.org/wp#Curation:AnalysisCollection> ;
                 wp:organism <http://purl.obolibrary.org/obo/NCBITaxon_9606> .
              }"""

    qres = temp.query(wp_query)
    for row in qres:
        print("%s" % row)
        wpids.append(str(row[0]))

    for pathway_id in wpids:
        try:
            run_one(pathway_id, retrieved, fast_run, write, login, temp)
        except Exception as e:
            traceback.print_exc()
            wdi_core.WDItemEngine.log("ERROR", wdi_helpers.format_msg(
                pathway_id, PROPS['Wikipathways ID'], None, str(e), type(e)))

def run_one(pathway_id, retrieved, fast_run, write, login, temp):
    print(pathway_id)
    pathway_reference = create_reference(pathway_id, retrieved)
    prep = dict()

    prep = get_PathwayElements(pathway=pathway_id,datatype="Metabolite", temp=temp, prep=prep)
    prep = get_PathwayElements(pathway=pathway_id, datatype="GeneProduct",temp=temp, prep=prep)
    # P703 = found in taxon, Q15978631 = "Homo sapiens"
    prep["P703"] = [
        wdi_core.WDItemID(value="Q15978631", prop_nr='P703', references=[copy.deepcopy(pathway_reference)])]

    query = """
            PREFIX wp:    <http://vocabularies.wikipathways.org/wp#>
            PREFIX gpml:    <http://vocabularies.wikipathways.org/gpml#>
            PREFIX dcterms: <http://purl.org/dc/terms/>
        SELECT DISTINCT ?pathway ?pwId ?pwLabel
        WHERE {
           VALUES ?pwId {"""
    query += "\"" + pathway_id + "\"^^xsd:string}"
    query += """
           ?pathway a wp:Pathway ;
                    dc:title ?pwLabel ;
                    dcterms:identifier ?pwId ;
                    <http://vocabularies.wikipathways.org/wp#isAbout> ?details ;
                    wp:organismName "Homo sapiens"^^xsd:string .
        }"""
    # print(query)
    qres3 = temp.query(query)

    for row in qres3:
        print(row[1])
        print(str(row[2]))
        # P31 = instance of
        prep["P31"] = [
            wdi_core.WDItemID(value="Q4915012", prop_nr="P31", references=[copy.deepcopy(pathway_reference)])]

        # P2410 = WikiPathways ID
        prep["P2410"] = [wdi_core.WDString(pathway_id, prop_nr='P2410', references=[copy.deepcopy(pathway_reference)])]

        # P2888 = exact match
        prep["P2888"] = [wdi_core.WDUrl("http://identifiers.org/wikipathways/" + str(row[1]), prop_nr='P2888',
                                        references=[copy.deepcopy(pathway_reference)])]

        query = """
                PREFIX wp:    <http://vocabularies.wikipathways.org/wp#>
                PREFIX dcterms: <http://purl.org/dc/terms/>
                select ?pubmed

                WHERE {
                 ?pubmed  a       wp:PublicationReference ;
                        dcterms:isPartOf <"""

        query += str(row[0])
        query += """> .}

                """
        qres4 = temp.query(query)
        print(query)

        for pubmed_result in qres4:
            pprint.pprint(pubmed_result)

            pmid = str(pubmed_result[0]).replace("http://identifiers.org/pubmed/", "")
            print(pmid)
            pmid_qid, _, _ = wdi_helpers.PublicationHelper(pmid.replace("PMID:", ""), id_type="pmid",
                                                     source="europepmc").get_or_create(login if write else None)
            if pmid_qid  is None:
                return panic(pathway_id, "not found: {}".format(pmid), "pmid")
            else:
                print(pmid_qid)
                if 'P2860' not in prep.keys():
                    prep["P2860"] = []
                prep['P2860'].append(wdi_core.WDItemID(value=str(pmid_qid), prop_nr='P2860', references=[copy.deepcopy(pathway_reference)]))

        data2add = []
        for key in prep.keys():
            for statement in prep[key]:
                data2add.append(statement)
                print(statement.prop_nr, statement.value)
        pprint.pprint(data2add)
        wdPage = wdi_core.WDItemEngine(data=data2add,
                                     domain="pathways",
                                     fast_run=fast_run,
                                     item_name=row.pwLabel,
                                     fast_run_base_filter=fast_run_base_filter,
                                     fast_run_use_refs=True,
                                     ref_handler=update_retrieved_if_new_multiple_refs,
                                     core_props=core_props)

        wdPage.set_label(str(row[2]), lang="en")
        wdPage.set_description("biological pathway in human", lang="en")

        try_write(wdPage, record_id=pathway_id, record_prop=PROPS['Wikipathways ID'],
                edit_summary="Updated a Wikipathways pathway", login=login, write=write)

def get_PathwayElements(pathway, datatype, temp, prep):
    wikidata_sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    query = """PREFIX wp:      <http://vocabularies.wikipathways.org/wp#>
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
    query += "\"" + pathway + "\"^^xsd:string .}"
    qres2 = temp.query(query)

    ids = []
    for row in qres2:
        ids.append("\"" + str(row[2]).replace("http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID", "").replace(
            "http://identifiers.org/ncbigene/", "") + "\"")


    # Check for existence of the ids in wikidata
    wd_query = "SELECT DISTINCT * WHERE {VALUES ?id {"
    wd_query += " ".join(ids)
    if datatype == "Metabolite":
        wd_query += "} ?item wdt:P662 ?id . }"
    if datatype == "GeneProduct":
        wd_query += "} ?item wdt:P351 ?id . }"

    wikidata_sparql.setQuery(wd_query)
    # print(wd_query)

    wikidata_sparql.setReturnFormat(JSON)
    results = wikidata_sparql.query().convert()
    for result in results["results"]["bindings"]:
        if "P527" not in prep.keys():
            prep["P527"] = []
        pathway_reference = create_reference(pathway, retrieved)
        prep["P527"].append(
            wdi_core.WDItemID(result["item"]["value"].replace("http://www.wikidata.org/entity/", ""),
                              prop_nr='P527', references=[copy.deepcopy(pathway_reference)]))
    return prep

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='run wikipathways bot')
    parser.add_argument('--dummy', help='do not actually do write', action='store_true')
    parser.add_argument('--no-fastrun', action='store_true')
    args = parser.parse_args()
    log_dir = "./logs"
    run_id = datetime.now().strftime('%Y%m%d_%H:%M')
    __metadata__['run_id'] = run_id
    fast_run = False if args.no_fastrun else False
    retrieved = datetime.now()

    log_name = '{}-{}.log'.format(__metadata__['name'], run_id)
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(__metadata__),
                                        logger_name='wikipathways')

    main(retrieved, fast_run=fast_run, write=not args.dummy)