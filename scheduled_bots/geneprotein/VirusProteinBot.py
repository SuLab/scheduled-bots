from wikidataintegrator import wdi_core, wdi_login
import copy
import os
from datetime import datetime
import json
import pprint
import requests
from rdflib import Graph, URIRef

import ftplib
import urllib.request
import gzip
import re

"""
Authors:
  Jasper Koehorst (ORCID:0000-0001-8172-8981 )
  Andra Waagmeester (ORCID:0000-0001-9773-4008)
  Egon Willighagen (ORCID:0000-0001-7542-0286)

This bot uses of the WikidataIntegrator.

Taxa ran: 694009, 1335626, 277944, 11137

This bot is a first attempt to automatically maintain genomics data on Wikidata from authoritittive resources on the 
SARS-CoV-2 virus. SARS-CoV-2 belongs to the broad family of viruses known as coronaviruses. This bot addresses the
seven known coronavirus to infect people.

The bot roughly works as follows:
1. Check if the taxonid of the virus is already covered in Wikidata
2. Get list of genes from https://mygene.info/
3. Create or check items on Wikidats for each annotated gene

The bot aligns with the following schema: https://www.wikidata.org/wiki/EntitySchema:E169
"""

## Functions to create references

print("Logging in...")
if "WDUSER" in os.environ and "WDPASS" in os.environ:
      WDUSER = os.environ['WDUSER']
      WDPASS = os.environ['WDPASS']
else:
      raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

login = wdi_login.WDLogin(WDUSER, WDPASS)

def createNCBIGeneReference(ncbiGeneId, retrieved):
    refStatedIn = wdi_core.WDItemID(value="Q20641742", prop_nr="P248", is_reference=True)
    timeStringNow = retrieved.strftime("+%Y-%m-%dT00:00:00Z")
    refRetrieved = wdi_core.WDTime(timeStringNow, prop_nr="P813", is_reference=True)
    refNcbiGeneID = wdi_core.WDString(value=ncbiGeneId, prop_nr="P351", is_reference=True)

    ncbi_reference = [refStatedIn, refRetrieved, refNcbiGeneID]
    return ncbi_reference

def createUniprotReference(uniprotId, retrieved):
    refStatedIn = wdi_core.WDItemID(value="Q905695", prop_nr="P248", is_reference=True)
    timeStringNow = retrieved.strftime("+%Y-%m-%dT00:00:00Z")
    refRetrieved = wdi_core.WDTime(timeStringNow, prop_nr="P813", is_reference=True)
    refUniprotID = wdi_core.WDString(value=uniprotId, prop_nr="P352", is_reference=True)

    reference = [refStatedIn, refRetrieved, refUniprotID]
    return reference

def getGeneQid(ncbiId):
    # Parent taxon
    gene_statements = [
    wdi_core.WDString(value=ncbiId, prop_nr="P351", references=[copy.deepcopy(ncbi_reference)])]
    return wdi_core.WDItemEngine(data=gene_statements)

def getTaxonItem(taxonQid):
    return wdi_core.WDItemEngine(wd_item_id=taxonQid)

def create_or_update_protein_item(geneid, uniprotID):
    retrieved = datetime.now()
    ncbi_reference = createNCBIGeneReference(hit["entrezgene"], retrieved)
    uniprot_reference = createUniprotReference(uniprotID, retrieved)
    query = """
                        PREFIX uniprotkb: <http://purl.uniprot.org/uniprot/>

                        SELECT * WHERE {
                            SERVICE <https://sparql.uniprot.org/sparql> {
                                 VALUES ?database {<http://purl.uniprot.org/database/PDB> <http://purl.uniprot.org/database/RefSeq>}
                                uniprotkb:"""
    query += uniprot
    query += """ rdfs:label ?label ;
                                     rdfs:seeAlso ?id .
                                 ?o <http://purl.uniprot.org/core/database> ?database .
                                 }
                                    }"""

    results = wdi_core.WDItemEngine.execute_sparql_query(query)
    refseq = []
    pdb = []
    for result in results["results"]["bindings"]:
        if not protein_label:
            protein_label = result["label"]["value"]
        if result["database"]["value"] == "http://purl.uniprot.org/database/RefSeq":
            if result["id"]["value"].replace("http://purl.uniprot.org/refseq/", "") not in refseq:
                refseq.append(result["id"]["value"].replace("http://purl.uniprot.org/refseq/", ""))
        if result["database"]["value"] == "http://purl.uniprot.org/database/RefSeq":
            if result["id"]["value"].replace("http://purl.uniprot.org/PDB/", "") not in refseq:
                refseq.append(result["id"]["value"].replace("http://rdf.wwpdb.org/pdb/", ""))

    statements = []

    # Instance of protein
    statements.append(wdi_core.WDItemID(value="Q8054", prop_nr="P31", references=[copy.deepcopy(uniprot_reference)]))

    # encoded by
    geneitem = getGeneQid(geneid)
    geneqid = geneitem.wd_item_id
    statements.append(wdi_core.WDItemID(value=geneqid, prop_nr="P702", references=[copy.deepcopy(ncbi_reference)]))

    # found in taxon
    geneJson = geneitem.get_wd_json_representation()
    taxonQID = geneJson['claims']["P703"][0]["mainsnak"]["datavalue"]["value"]["id"]
    statements.append(wdi_core.WDItemID(taxonQID, prop_nr="P703", references=[copy.deepcopy(ncbi_reference)]))

    # exactMatch
    statements.append(wdi_core.WDUrl("http://purl.uniprot.org/uniprot/"+uniprotID, prop_nr="P2888",  references=[copy.deepcopy(uniprot_reference)]))

    ## Identifier statements
    # uniprot
    statements.append(wdi_core.WDString(id, prop_nr="P352", references=[copy.deepcopy(uniprot_reference)]))

    # refseq
    for id in refseq:
        statements.append(wdi_core.WDString(id, prop_nr="P637", references=[copy.deepcopy(uniprot_reference)]))

    # pdb
    for id in pdb:
        statements.append(wdi_core.WDString(id, prop_nr="P638", references=[copy.deepcopy(uniprot_reference)]))
    taxonname = getTaxonItem(geneJson['claims']["P703"][0]["mainsnak"]["datavalue"]["value"]["id"]).get_label(lang="en")
    protein_item = wdi_core.WDItemEngine(data=statements)
    if protein_item.get_label(lang="en") == "":
        protein_item.set_label(protein_label, lang="en")
    if protein_item.get_description(lang="en") == "":
        protein_item.set_description("protein in "+taxonname, lang="en")
    if protein_item.get_description(lang="de") == "":
        protein_item.set_description("Eiweiß in "+taxonname+" gefunden", lang="de")
    if protein_item.get_description(lang="nl") == "":
        protein_item.set_description("Eiwit in "+taxonname, lang="nl")
    if protein_item.get_description(lang="es") == "":
        protein_item.set_description("proteína encontrada en "+taxonname, lang="es")
    if protein_item.get_description(lang="it") == "":
        protein_item.set_description("Proteina in " + taxonname, lang="it")

    print(protein_item.get_wd_json_representation())
    protein_qid = protein_item.write(login)
    print(protein_qid)
    ## add the newly create protein item to the gene item
    encodes = [wdi_core.WDItemID(protein_qid, prop_nr="P688", references=[copy.deepcopy(ncbi_reference)])]
    geneitem = wdi_core.WDItemEngine(wd_item_id=geneqid, data=encodes)
    geneitem.write(login)


taxid = "11137"
genelist = json.loads(requests.get("https://mygene.info/v3/query?q=*&species="+taxid).text)

for hit in genelist["hits"]:
    print(hit["entrezgene"])
    geneinfo = json.loads(requests.get("http://mygene.info/v3/gene/" + hit["entrezgene"]).text)
    # uniprot identifer
    if "uniprot" in geneinfo.keys():
        if "Swiss-Prot" in geneinfo["uniprot"]:
            if isinstance(geneinfo["uniprot"]["Swiss-Prot"], list):
                for uniprot in geneinfo["uniprot"]["Swiss-Prot"]:
                    print(uniprot +": "+create_or_update_protein_item(hit["entrezgene"], uniprot))
            else:
                print(uniprot +": "+create_or_update_protein_item(hit["entrezgene"], uniprot))





