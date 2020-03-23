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

Taxa ran: 2697049, 1415852, 227859, 349342, 305407, 1335626

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

"""
Authors:
  Jasper Koehorst (ORCID:0000-0001-8172-8981 )
  Andra Waagmeester (ORCID:0000-0001-9773-4008)
  Egon Willighagen (ORCID:0000-0001-7542-0286)

This bot uses of the WikidataIntegrator.

Taxa ran: 2697049, 1415852, 227859, 349342, 305407, 1335626
"""

def getGeneQid(ncbiId):
    # Parent taxon
    gene_statements = [
    wdi_core.WDString(value=ncbiId, prop_nr="P351", references=[copy.deepcopy(ncbi_reference)])]
    item = wdi_core.WDItemEngine(data=gene_statements)
    return item.wd_item_id


taxid = "290028"
genelist = json.loads(requests.get("https://mygene.info/v3/query?q=*&species="+taxid).text)

retrieved = datetime.now()

for hit in genelist["hits"]:
    print(hit["entrezgene"])
    protein_label = None
    statements = []
    geneinfo = json.loads(requests.get("http://mygene.info/v3/gene/" + hit["entrezgene"]).text)
    pprint.pprint(geneinfo)
    ncbi_reference = createNCBIGeneReference(hit["entrezgene"], retrieved)
    # Instance of protein
    statements.append(wdi_core.WDItemID(value="Q8054", prop_nr="P31", references=[copy.deepcopy(ncbi_reference)]))

    # encoded by
    geneqid = getGeneQid(hit["entrezgene"])
    statements.append(wdi_core.WDItemID(value=geneqid, prop_nr="P702", references=[copy.deepcopy(ncbi_reference)]))

    ## Identifiers
    #refseq
    if "refseq" in geneinfo.keys():
        if isinstance(geneinfo["refseq"]["protein"], list):
            for id in geneinfo["refseq"]["protein"]:
                statements.append(wdi_core.WDString(id, prop_nr="P637", references=[copy.deepcopy(ncbi_reference)]))
        else:
            statements.append(wdi_core.WDString(geneinfo["refseq"]["protein"], prop_nr="P637", references=[copy.deepcopy(ncbi_reference)]))

    # uniprot identifer
    if "uniprot" in geneinfo.keys():
        uniprot_reference = createUniprotReference(geneinfo["uniprot"]["Swiss-Prot"], retrieved)

        query = "PREFIX uniprotkb: <http://purl.uniprot.org/uniprot/> PREFIX uc: <http://purl.uniprot.org/core/> SELECT * WHERE { SERVICE <https://sparql.uniprot.org/sparql> { uniprotkb:"+geneinfo["uniprot"]["Swiss-Prot"]+" rdfs:label ?label .}}"
        print(query)
        result = wdi_core.WDItemEngine.execute_sparql_query(query)
        protein_label = result["results"]["bindings"][0]["label"]["value"] ## TODO make this cleaner
        statements.append(wdi_core.WDString(geneinfo["uniprot"]["Swiss-Prot"], prop_nr="P352", references=[copy.deepcopy(ncbi_reference)]))

    # PDB identifer
    if "pdb" in geneinfo.keys():
        if isinstance(geneinfo["pdb"], list):
            for id in geneinfo["pdb"]:
                statements.append(wdi_core.WDExternalID(id, prop_nr="P638", references=[copy.deepcopy(ncbi_reference)]))
        else:
            statements.append(wdi_core.WDExternalID(geneinfo["pdb"], prop_nr="P638", references=[copy.deepcopy(ncbi_reference)]))


    protein_item = wdi_core.WDItemEngine(data=statements)

    if protein_label:
        protein_item.set_label(protein_label, lang="en")
        print(protein_item.get_wd_json_representation())
        protein_qid = protein_item.write(login)
        print(protein_qid)
        encodes = [wdi_core.WDItemID(protein_qid, prop_nr="P688", references=[copy.deepcopy(ncbi_reference)])]
        geneitem = wdi_core.WDItemEngine(wd_item_id=geneqid, data=encodes)
        geneitem.write(login)

