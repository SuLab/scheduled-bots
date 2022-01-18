from wikidataintegrator import wdi_core, wdi_rdf, wdi_login
import mygene
from datetime import datetime
import copy
import os

print("Logging in...")
if "WDUSER" in os.environ and "WDPASS" in os.environ:
    WDUSER = os.environ['WDUSER']
    WDPASS = os.environ['WDPASS']
else:
    raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

login = wdi_login.WDLogin(WDUSER, WDPASS)

mg = mygene.MyGeneInfo()
metadata = mg.metadata()

source_items = {'uniprot': 'Q905695',
                'ncbi_gene': 'Q20641742',  # these two are the same
                'entrez': 'Q20641742',
                'ncbi_taxonomy': 'Q13711410',
                'swiss_prot': 'Q2629752',
                'trembl': 'Q22935315',
                'ensembl': 'Q1344256',
                'refseq': 'Q7307074',
                'mygeneinfo': 'Q74843110'}

qid = {
    "gene": 'Q7187',
    "protein-coding": 'Q20747295',
}

pid = {
    "instance of": "P31",
    "subclass of": "P279",
    "reference url": "P854",
    "found in taxon": "P703",
}

source_properties = {
    "entrez" : "P351" , # Entrez Gene ID
    "homologene" : "P593" , # HomoloGene ID
    "pharmgkb" : "P7001" , # PharmGKB ID
    "ensembl.transcript" : "P704" , # Ensembl transcript ID
    "ensembl.gene" : "P594" , # Ensembl gene ID
    "reactome" : "P3937" , # Reactome ID
    "ucsc" : "P2576" , # UCSC Genome Browser assembly ID
    "umls" : "P2892" , # UMLS CUI
    "refseq.genome" : "P2249" , # RefSeq genome ID
    "refseq.protein" : "P637" , # RefSeq protein ID
    "refseq.rna" : "P639" , # RefSeq RNA ID
    "refseq" : "P656" , # RefSeq
    "uniprot" : "P352" , # UniProt protein ID
    "hgnc": "P354", # HGNC ID
}
retrieved = datetime.strptime(metadata["build_version"], "%Y%m%d")

def make_reference(source, id_prop, identifier, retrieved):
    reference = [
          wdi_core.WDItemID(value=source_items[source], prop_nr='P248', is_reference=True),  # stated in
          wdi_core.WDString(value=str(identifier), prop_nr=id_prop, is_reference=True),  # Link to ID
          wdi_core.WDTime(retrieved.strftime('+%Y-%m-%dT00:00:00Z'), prop_nr='P813', is_reference=True),]
    return reference

entrez_query = "SELECT * WHERE {?item wdt:P351 ?entrezgeneid .}"
df =  wdi_core.WDFunctionsEngine.execute_sparql_query(entrez_query, as_dataframe=True)
entrez_qid = dict()
for index, row in df.iterrows():
  entrez_qid[row["entrezgeneid"]] = row["item"].replace("http://www.wikidata.org/entity/", "")
print("taxa")
taxon_query = 'SELECT * WHERE { ?wdtaxon wdt:P685 ?taxid .}'
df = wdi_core.WDFunctionsEngine.execute_sparql_query(taxon_query, as_dataframe=True)
taxon_qid = dict()
for index, row in df.iterrows():
  taxon_qid[row["taxid"]] = row["wdtaxon"].replace("http://www.wikidata.org/entity/", "")




def entrez2wikidata(entrezid):
    entrez_reference = make_reference("entrez", source_properties['entrez'], entrezid, retrieved)
    geneinfo = mg.getgene(entrezid)
    # print(geneinfo)
    statements = []
    if not geneinfo:
        # instance of gene
        statements.append(
            wdi_core.WDItemID(qid["gene"], prop_nr=pid["instance of"], references=[copy.deepcopy(entrez_reference)],
                              rank='deprecated'))
        item = wdi_core.WDItemEngine(wd_item_id=entrez_qid[entrezid], data=statements)

        for prop in item.wd_json_representation["claims"].keys():
            if prop != "P351":
                for stat in item.wd_json_representation["claims"][prop]:
                    stat["rank"] = "deprecated"
        print(item.write(login))
    else:
        # instance of gene
        statements.append(
            wdi_core.WDItemID(qid["gene"], prop_nr=pid["instance of"], references=[copy.deepcopy(entrez_reference)]))

        # subclass of type of gene
        if "type_of_gene" in geneinfo.keys():
            if geneinfo["type_of_gene"] == "protein-coding":
                statements.append(wdi_core.WDItemID(qid["protein-coding"], prop_nr=pid["subclass of"],
                                                    references=[copy.deepcopy(entrez_reference)]))

        # found in taxon
        if "taxid" in geneinfo.keys():
            statements.append(wdi_core.WDItemID(taxon_qid[str(geneinfo["taxid"])], prop_nr=pid["found in taxon"],
                                                references=[copy.deepcopy(entrez_reference)]))

        # identifiers
        ## entrez
        if "entrezgene" in geneinfo.keys():
            statements.append(wdi_core.WDExternalID(entrezid, prop_nr=source_properties['entrez'],
                                                    references=[copy.deepcopy(entrez_reference)]))

        ## ensembl
        if "ensembl" in geneinfo.keys():
            if "gene" in geneinfo["ensembl"]:
                statements.append(
                    wdi_core.WDExternalID(geneinfo["ensembl"]["gene"], prop_nr=source_properties["ensembl.gene"],
                                          references=[copy.deepcopy(entrez_reference)]))

        ## hgnc
        if "hgnc" in geneinfo.keys():
            statements.append(wdi_core.WDExternalID(geneinfo["HGNC"], prop_nr=source_properties["HGNC ID"],
                                                    references=[copy.deepcopy(entrez_reference)]))

        item = wdi_core.WDItemEngine(wd_item_id=entrez_qid[entrezid], data=statements)
        print(item.write(login))


query = """
SELECT * WHERE {
     ?gene wdt:P351 ?ncbi_gene .
     FILTER NOT EXISTS {{?gene p:P31/ps:P31 ?instance } UNION {?gene wdt:P279 ?subclass}}
} LIMIT 10
"""

results = wdi_core.WDFunctionsEngine.execute_sparql_query(query, as_dataframe=True)
for index, row in results.iterrows():
    entrez2wikidata(row["ncbi_gene"])