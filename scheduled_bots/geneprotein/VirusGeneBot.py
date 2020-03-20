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

"""

def createReference(ncbiGeneId, retrieved):
    refStatedIn = wdi_core.WDItemID(value="Q20641742", prop_nr="P248", is_reference=True)
    timeStringNow = retrieved.strftime("+%Y-%m-%dT00:00:00Z")
    refRetrieved = wdi_core.WDTime(timeStringNow, prop_nr="P813", is_reference=True)
    refNcbiGeneID = wdi_core.WDString(value=ncbiGeneId, prop_nr="P351", is_reference=True)

    ncbi_reference = [refStatedIn, refRetrieved, refNcbiGeneID]
    return ncbi_reference

retrieved = datetime.now()

print("Logging in...")
if "WDUSER" in os.environ and "WDPASS" in os.environ:
    WDUSER = os.environ['WDUSER']
    WDPASS = os.environ['WDPASS']
else:
    raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

login = wdi_login.WDLogin(WDUSER, WDPASS)

taxid = "2697049"
genelist = json.loads(requests.get("https://mygene.info/v3/query?q=*&species="+taxid).text)


for hit in genelist["hits"]:
  ncbi_reference = createReference(hit["entrezgene"], retrieved)
  print(hit["entrezgene"])
  geneinfo = json.loads(requests.get("http://mygene.info/v3/gene/"+hit["entrezgene"]).text)

  reference = []
  statements = []

  # instance of gene

  statements.append(wdi_core.WDItemID(value="Q7187", prop_nr="P31", references=[copy.deepcopy(ncbi_reference)]))

  if geneinfo["type_of_gene"] == "protein-coding":
      statements.append(wdi_core.WDItemID(value="Q20747295", prop_nr="P279", references=[copy.deepcopy(ncbi_reference)]))
  # found in taxon
  statements.append(wdi_core.WDItemID(value="Q82069695", prop_nr="P703", references=[copy.deepcopy(ncbi_reference)]))


  ## identifiers
  # ncbi locus tag identifer
  if "locus_tag" in geneinfo.keys():
    statements.append(wdi_core.WDString(geneinfo["locus_tag"], prop_nr="P2393", references=[copy.deepcopy(ncbi_reference)]))

  # ncbi identifer
  statements.append(wdi_core.WDString(geneinfo["entrezgene"], prop_nr="P351", references=[copy.deepcopy(ncbi_reference)]))


  item = wdi_core.WDItemEngine(data=statements)
  item.set_label(geneinfo["name"], lang="en")
  item.set_description("SARS-CoV-2 gene", lang="en") ## TODO needs to be changed when running other genes

  #pprint.pprint(item.get_wd_json_representation())
  print(item.write(login))
  # item.write(login)

