from wikidataintegrator import wdi_core, wdi_login
import copy
import os
from datetime import datetime
import json
import pprint
import requests

import ftplib
import urllib.request
import gzip
from Bio import SeqIO

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

The bot aligns with the following schema: https://www.wikidata.org/wiki/EntitySchema:E165
"""


## Functions to create references
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


retrieved = datetime.now() # Get currentdate to use as timestamp

def main():
  ## Login to Wikidata
  print("Logging in...")
  if "WDUSER" in os.environ and "WDPASS" in os.environ:
      WDUSER = os.environ['WDUSER']
      WDPASS = os.environ['WDPASS']
  else:
      raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

  login = wdi_login.WDLogin(WDUSER, WDPASS)

  ## Provide the taxonomy id from NCBI taxonomy and create or update the related Wikidata item
  taxid = "1335626"
  ncbiTaxref = createNCBITaxReference(taxid, retrieved)
  ncbiTaxon = json.loads(requests.get("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=taxonomy&id={}&format=json".format(taxid)).text)

  taxonitemStatements = []
  ## instance of
  taxonitemStatements.append(wdi_core.WDItemID(value="Q16521", prop_nr="P31", references=[copy.deepcopy(ncbiTaxref)]))
  ## NCBI tax id
  taxonitemStatements.append(wdi_core.WDExternalID(value=taxid, prop_nr="P685", references=[copy.deepcopy(ncbiTaxref)]))
  ## scientificname
  scientificName = ncbiTaxon["result"][taxid]['scientificname']
  taxonitemStatements.append(wdi_core.WDString(scientificName, prop_nr="P225", references=[copy.deepcopy(ncbiTaxref)]))
  item = wdi_core.WDItemEngine(data=taxonitemStatements)
  if item.get_label() == "":
      item.set_label(label=scientificName, lang="en")
  if item.get_label() != scientificName:
      item.set_aliases(aliases=[scientificName])
  if item.get_description(lang="en") == "":
      item.set_description(description="strain of virus", lang="en")
  found_in_taxID = item.write(login)
  print(found_in_taxID)

  ## Get list of genes from https://mygene.info/ and create or update items on Wikidats for each annotated gene
  genelist = json.loads(requests.get("https://mygene.info/v3/query?q=*&species="+taxid).text)
  pprint.pprint(genelist)
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

    #pprint.pprint(item.get_wd_json_representation()) ## get json for test purposes
    print(item.write(login)) # write the wikidata item and return the QID


def refseq():
  # The tax id to take the refseq from
  taxid = "1335626"
  # assembly_summary_refseq.txt
  # Download file: https://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_refseq.txt
  for index, line in enumerate(open("assembly_summary_refseq.txt")):
    line = line.split("\t")
    if len(line) < 10: continue
    # When taxid matches and it is a reference genome download the genbank file from the NCBI
    if line[5] == taxid and line[4] == "reference genome":
      ftp_full_path = line[-3]
      ftp_path = '/' + '/'.join(ftp_full_path.split("/")[3:])
      ftp = ftplib.FTP('ftp.ncbi.nlm.nih.gov')
      ftp.login()
      ftp.cwd(ftp_path)
      dir_list = []
      ftp.dir(dir_list.append)
      for ftp_line in dir_list:
        if ftp_line.endswith("_genomic.gbff.gz"):
          ftp_line = ftp_line.split()[-1]
          urllib.request.urlretrieve(ftp_full_path + "/" + ftp_line, ftp_line)
          # Start genbank parsing
          parse_genbank(ftp_line)


def parse_genbank(genbank_file):
  statements = []
  # Genbank file is compressed, read directly from gzip
  handle = gzip.open(genbank_file, 'rt')
  # Parse as genbank file
  for seq_record in SeqIO.parse(handle, "genbank"):
      print(seq_record.id)
      print(repr(seq_record.seq))
      print(len(seq_record))
      for feature in seq_record.features:
        if feature.type == "gene":
          statements += location(feature)
          statements += qualifiers(feature)

def qualifiers(feature):
  statements = []
  gene = feature.qualifiers['gene']
  locus_tag = feature.qualifiers['locus_tag']
  statements += xref(feature.qualifiers['db_xref'])
  return statements


def xref(db_xref):
  statements = []
  return statements


def location(feature):
  statements = []
  # Add the location statements
  strand = feature.location.strand
  start = feature.location.start
  end = feature.location.end
  return statements


if __name__ == '__main__':
  # main()
  refseq()
