from wikidataintegrator import wdi_core, wdi_login
import copy
import os
import datetime
import json
import pprint

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

lgoin = wdi_login.WDLogin(user=WDUSER, pass=WDPASS)

taxid = "2697049"
genelist = json.loads(requests.get("https://mygene.info/v3/query?q=*&species="+taxid).text)


for hit in genelist["hits"]:
  ncbi_reference = createReference(hit["entrezgene"], retrieved)
  print(hit["entrezgene"])
  geneinfo = json.loads(requests.get("http://mygene.info/v3/gene/"+hit["entrezgene"]).text)

  reference = []
  statements = []

  # instance of gene
  statements.append(wdi_core.WDItemID(value="Q20747295", prop="P31", references=[copy.deepcopy(ncbi_reference)]))

  # found in taxon
  statements.append(wdi_core.WDItemID(taxid, prop="P703", references=[copy.deepcopy(ncbi_reference)]))

  # encodes

  ## identifiers
  # ncbi identifer
  statements.append(wdi_core.WDString(geneinfo["entrezgene"], prop="P351", references=[copy.deepcopy(ncbi_reference)]))
  statements.append(wdi_core.WDItemID(""))

  item = wdi_core.WDItemEngine()
  item.set_label(gene_info["name"], lang="en")
  item.set_description("", lang="en")

  pprint.pprint(item.get_wd_json_representation())
  # item.write(login)

