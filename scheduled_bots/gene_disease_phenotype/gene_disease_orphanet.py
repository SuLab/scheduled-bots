"""
Gene to disease (orphanet)
http://www.orphadata.org/cgi-bin/inc/product6.inc.php
http://www.orphadata.org/data/xml/en_product6.xml


In wikidata
gene -> genetic association (P2293) -> disease

"""

import xml.etree.ElementTree as et
import pandas as pd
from itertools import chain

from tqdm import tqdm

from scheduled_bots import PROPS
from wikidataintegrator import wdi_core, wdi_helpers, wdi_login
from wikicurie import wikicurie
from scheduled_bots.local import WDUSER, WDPASS
from wikidataintegrator.ref_handlers import update_retrieved_if_new_multiple_refs

wd_login = wdi_login.WDLogin(WDUSER, WDPASS)
WIKIBASE = True
if WIKIBASE:
    from wikibase_tools import EntityMaker

    mediawiki_api_url = "http://localhost:7171/w/api.php"
    sparql_endpoint_url = "http://localhost:7272/proxy/wdqs/bigdata/namespace/wdq/sparql"
    mediawiki_api_url = "http://185.54.114.71:8181/w/api.php"
    sparql_endpoint_url = "http://185.54.114.71:8282/proxy/wdqs/bigdata/namespace/wdq/sparql"
    username = "testbot"
    password = "password"
    maker = EntityMaker(mediawiki_api_url, sparql_endpoint_url, username, password)
    item_engine = wdi_core.WDItemEngine.wikibase_item_engine_factory(mediawiki_api_url, sparql_endpoint_url)
    login = wdi_login.WDLogin("testbot", "password", mediawiki_api_url=mediawiki_api_url)
else:
    sparql_endpoint_url = 'https://query.wikidata.org/sparql'
    item_engine = wdi_core.WDItemEngine
    login = wd_login


cu = wikicurie.CurieUtil()
h = wdi_helpers.WikibaseHelper(sparql_endpoint_url=sparql_endpoint_url)

for k, v in PROPS.items():
    try:
        PROPS[k] = h.get_pid(v)
    except Exception:
        PROPS[k] = None

ORPHANET_QID = h.get_qid("Q1515833")

EXT_ID_MAP = dict()

def retrieve_qid_from_curie(curie):
    pid, ext_id_value = cu.parse_curie(curie)
    pid = h.get_pid(pid)
    if pid not in EXT_ID_MAP:
        EXT_ID_MAP[pid] = wdi_helpers.id_mapper(pid, endpoint=h.sparql_endpoint_url)
    if ext_id_value not in EXT_ID_MAP[pid]:
        print("Curie not found: {}".format(curie))
        return None
    qid = EXT_ID_MAP[pid][ext_id_value]
    return qid

def parse_orpha_gene_disease():
    # modified from here: https://github.com/biothings/mydisease.info/blob/master/mydisease/dataload/orphanet/parser.py
    tree = et.parse("en_product6.xml")
    root = tree.getroot()
    gene_d = {}
    dgass = []
    for disease in root.find("DisorderList"):
        orpha = "ORPHA:" + disease.find("OrphaNumber").text
        genes = disease.findall("GeneList/Gene")
        for gene in genes:
            synonyms = [x.text for x in gene.findall("SynonymList/Synonym")]
            gene_type = gene.find("GeneType/Name").text
            loci = [x.find("GeneLocus").text for x in gene.findall("LocusList/Locus")]
            gene_d[gene.attrib['id']] = {'synonyms': synonyms, 'gene_type': gene_type, 'loci': loci}

        dg_associations = disease.findall("DisorderGeneAssociationList/DisorderGeneAssociation")
        for dga in dg_associations:
            gene = dga.find("Gene")
            gene_name = gene.find("Name").text
            gene_symbol = gene.find("Symbol").text
            dga_type = dga.find("DisorderGeneAssociationType/Name").text
            dga_status = dga.find("DisorderGeneAssociationStatus/Name").text
            this_dga = {
                'gene_name': gene_name, 'gene_symbol': gene_symbol, 'dga_type': dga_type, 'dga_status': dga_status,
                'gene_type': gene_d[gene.attrib['id']]['gene_type'],
                'disease': orpha
            }
            dgass.append(this_dga)
    df = pd.DataFrame(dgass)
    return df


# gene -> disease (orpha)
df = parse_orpha_gene_disease()
all_hgnc = set(df.gene_symbol)

if WIKIBASE:
    all_hgnc_wd_qid = wdi_helpers.get_values("P353", all_hgnc)
    all_hgnc_qid = {hgnc: h.URI_QID.get("http://www.wikidata.org/entity/" + v) for hgnc, v in all_hgnc_wd_qid.items()}
    all_hgnc_qid = {k: v for k, v in all_hgnc_qid.items() if v}
else:
    all_hgnc_qid = wdi_helpers.get_values("P353", all_hgnc)

gb = df.groupby("gene_symbol")
gb = list(gb)
for gene_symbol, thisdf in tqdm(gb):
    gene_qid = all_hgnc_qid.get(gene_symbol)
    if not gene_qid:
        continue
    data = []
    for _, row in thisdf.iterrows():
        disease_curie = row.disease
        disease_qid = retrieve_qid_from_curie(disease_curie)
        if not disease_qid:
            continue
        ref = [wdi_core.WDItemID(ORPHANET_QID, PROPS['stated in'], is_reference=True),
               wdi_core.WDExternalID(disease_curie.split(":")[1], PROPS['Orphanet ID'], is_reference=True)]
        s = wdi_core.WDItemID(disease_qid, PROPS['genetic association'], references=[ref])
        data.append(s)

    item = item_engine(wd_item_id=gene_qid, data=data, append_value=PROPS['genetic association'],
                       global_ref_mode='CUSTOM', ref_handler=update_retrieved_if_new_multiple_refs)
    item.write(login)