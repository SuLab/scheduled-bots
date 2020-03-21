from wikidataintegrator import wdi_core, wdi_login
import copy
import os
from datetime import datetime
import json
import pprint
import requests

"""
Authors:
  Jasper Koehorst (ORCID:0000-0001-8172-8981 )
  Andra Waagmeester (ORCID:0000-0001-9773-4008)
  Egon Willighagen (ORCID:0000-0001-7542-0286)

This bot uses of the WikidataIntegrator.

Taxa ran: 2697049, 1415852, 227859, 349342
"""


def createNCBIGeneReference(ncbiGeneId, retrieved):
    refStatedIn = wdi_core.WDItemID(value="Q20641742", prop_nr="P248", is_reference=True)
    timeStringNow = retrieved.strftime("+%Y-%m-%dT00:00:00Z")
    refRetrieved = wdi_core.WDTime(timeStringNow, prop_nr="P813", is_reference=True)
    refNcbiGeneID = wdi_core.WDString(value=ncbiGeneId, prop_nr="P351", is_reference=True)

    ncbi_reference = [refStatedIn, refRetrieved, refNcbiGeneID]
    return ncbi_reference

def createNCBITaxReference(ncbiTaxId, retrieved):
    refStatedIn = wdi_core.WDItemID(value="Q13711410", prop_nr="P248", is_reference=True)
    timeStringNow = retrieved.strftime("+%Y-%m-%dT00:00:00Z")
    refRetrieved = wdi_core.WDTime(timeStringNow, prop_nr="P813", is_reference=True)
    refNcbiTaxID = wdi_core.WDString(value=ncbiTaxId, prop_nr="P685", is_reference=True)
    ncbi_reference = [refStatedIn, refRetrieved, refNcbiTaxID]
    return ncbi_reference

retrieved = datetime.now()

print("Logging in...")
if "WDUSER" in os.environ and "WDPASS" in os.environ:
    WDUSER = os.environ['WDUSER']
    WDPASS = os.environ['WDPASS']
else:
    raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

login = wdi_login.WDLogin(WDUSER, WDPASS)

taxid = "349342"
ncbiTaxref = createNCBITaxReference(taxid, retrieved)
genelist = json.loads(requests.get("https://mygene.info/v3/query?q=*&species="+taxid).text)
ncbiTaxon = json.loads(requests.get("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=taxonomy&id={}&format=json".format(taxid)).text)

pprint.pprint(genelist)
taxonitemStatements = []
## instance of
taxonitemStatements.append(wdi_core.WDItemID(value="Q16521", prop_nr="P31"))
## NCBI tax id
taxonitemStatements.append(wdi_core.WDExternalID(value=taxid, prop_nr="P685"))
## scientificname
scientificName = ncbiTaxon["result"][taxid]['scientificname']
taxonitemStatements.append(wdi_core.WDString(scientificName, prop_nr="P225", references=[copy.deepcopy(ncbiTaxref)]))
item = wdi_core.WDItemEngine(data=taxonitemStatements)
if item.get_label() == "":
    item.set_label(label=scientificName, lang="en")
if item.get_label() != scientificName:
    item.set_aliases(aliases=[scientificName])
found_in_taxID = item.write(login)
print(found_in_taxID)

for hit in genelist["hits"]:
  ncbi_reference = createNCBIGeneReference(hit["entrezgene"], retrieved)
  print(hit["entrezgene"])
  geneinfo = json.loads(requests.get("http://mygene.info/v3/gene/"+hit["entrezgene"]).text)

  reference = []
  statements = []

  # instance of gene

  statements.append(wdi_core.WDItemID(value="Q7187", prop_nr="P31", references=[copy.deepcopy(ncbi_reference)]))

  if geneinfo["type_of_gene"] == "protein-coding":
      statements.append(wdi_core.WDItemID(value="Q20747295", prop_nr="P279", references=[copy.deepcopy(ncbi_reference)]))
  # found in taxon
  statements.append(wdi_core.WDItemID(value=found_in_taxID, prop_nr="P703", references=[copy.deepcopy(ncbi_reference)]))


  ## identifiers
  # ncbi locus tag identifer
  if "locus_tag" in geneinfo.keys():
    statements.append(wdi_core.WDString(geneinfo["locus_tag"], prop_nr="P2393", references=[copy.deepcopy(ncbi_reference)]))

  # ncbi identifer
  statements.append(wdi_core.WDString(geneinfo["entrezgene"], prop_nr="P351", references=[copy.deepcopy(ncbi_reference)]))


  item = wdi_core.WDItemEngine(data=statements)
  item.set_label(geneinfo["name"], lang="en")
  item.set_description(scientificName+" gene", lang="en")

  #pprint.pprint(item.get_wd_json_representation())
  print(item.write(login))
  # item.write(login)

