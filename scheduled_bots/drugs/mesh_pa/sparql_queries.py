# load mesh pharmacological action data
# 13k drug -> PA pairs, on 9270 drugs, 532 PAs

# https://id.nlm.nih.gov/mesh/query?query=PREFIX+rdf%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23%3E%0D%0APREFIX+rdfs%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2000%2F01%2Frdf-schema%23%3E%0D%0APREFIX+xsd%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema%23%3E%0D%0APREFIX+owl%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2002%2F07%2Fowl%23%3E%0D%0APREFIX+meshv%3A+%3Chttp%3A%2F%2Fid.nlm.nih.gov%2Fmesh%2Fvocab%23%3E%0D%0APREFIX+mesh%3A+%3Chttp%3A%2F%2Fid.nlm.nih.gov%2Fmesh%2F%3E%0D%0APREFIX+mesh2015%3A+%3Chttp%3A%2F%2Fid.nlm.nih.gov%2Fmesh%2F2015%2F%3E%0D%0APREFIX+mesh2016%3A+%3Chttp%3A%2F%2Fid.nlm.nih.gov%2Fmesh%2F2016%2F%3E%0D%0APREFIX+mesh2017%3A+%3Chttp%3A%2F%2Fid.nlm.nih.gov%2Fmesh%2F2017%2F%3E%0D%0APREFIX+mesh2018%3A+%3Chttp%3A%2F%2Fid.nlm.nih.gov%2Fmesh%2F2018%2F%3E%0D%0A%0D%0ASELECT+*+%0D%0AFROM+%3Chttp%3A%2F%2Fid.nlm.nih.gov%2Fmesh%3E%0D%0AWHERE+%7B%0D%0A++mesh%3AD015242+meshv%3ApharmacologicalAction+%3Fpa+.%0D%0A++%3Fpa+rdfs%3Alabel+%3FpaLabel+.%0D%0A%7D+%0D%0A&format=HTML&year=current&limit=50&offset=0#lodestart-sparql-results

# format=HTML&year=current&limit=50&offset=0#lodestart-sparql-results
import pandas as pd
pd.set_option("display.width", 100)

from wikidataintegrator import wdi_helpers, wdi_core, wdi_login
mesh_qid = wdi_helpers.id_mapper("P486", return_as_set=True)
mesh_qid = {k: list(v)[0] for k, v in mesh_qid.items() if len(v) == 1}


import requests
url = "http://id.nlm.nih.gov/mesh/sparql"

prefix = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>
PREFIX mesh: <http://id.nlm.nih.gov/mesh/>
"""
# get drugs and pharm action
query = """SELECT * FROM <http://id.nlm.nih.gov/mesh> WHERE {
  ?mesh meshv:pharmacologicalAction ?pa .
  ?mesh meshv:active 1 .
  ?pa rdfs:label ?paLabel .
} limit 100"""
r = requests.get(url, params={'query': prefix + query, 'format': 'JSON'})


# get mesh pharm action parents
query = """SELECT distinct ?pa ?paLabel ?parent ?parentLabel FROM <http://id.nlm.nih.gov/mesh> WHERE {
  ?mesh meshv:pharmacologicalAction ?pa .
  ?mesh meshv:active 1 .
  ?pa rdfs:label ?paLabel .
  ?parent rdfs:label ?parentLabel .
  ?pa meshv:treeNumber ?treeNum .
  ?treeNum meshv:parentTreeNumber+ ?parentTreeNum .
  ?parent meshv:treeNumber ?parentTreeNum .
}"""
r = requests.get(url, params={'query': prefix + query, 'format': 'JSON'})
res = [{k:v['value'] for k,v in x.items()} for x in r.json()['results']['bindings']]
df = pd.DataFrame(res)
df.drop_duplicates(inplace=True)
df.sort_values("pa").head(15)


# get all children of D020164 Chemical Actions and Uses (D27)
query = """SELECT distinct * FROM <http://id.nlm.nih.gov/mesh> WHERE {
  FILTER(STRSTARTS(STR(?treeNum), "http://id.nlm.nih.gov/mesh/D27."))
  ?mesh meshv:treeNumber ?treeNum .
  ?mesh meshv:active 1 .
  ?mesh rdfs:label ?meshLabel .
}"""
r = requests.get(url, params={'query': prefix + query, 'format': 'JSON'})
res = [{k:v['value'] for k,v in x.items()} for x in r.json()['results']['bindings']]
df = pd.DataFrame(res)
df.sort_values("mesh").head(15)

### query to get counts of each PA
query = """
SELECT (COUNT(?d) as ?c) ?pa ?paLabel FROM <http://id.nlm.nih.gov/mesh> WHERE {
 ?d meshv:pharmacologicalAction ?pa.
 ?d meshv:active 1 .
 ?pa rdfs:label ?paLabel
} group by ?pa ?paLabel
order by DESC(?c)
"""
r = requests.get(url, params={'query': prefix + query, 'format': 'JSON'})
res = [{k:v['value'] for k,v in x.items()} for x in r.json()['results']['bindings']]

for r in res:
    r['qid'] = mesh_qid.get(r['pa'].replace("http://id.nlm.nih.gov/mesh/", ""))

df = pd.DataFrame(res)
df.c = list(map(int, df.c))
df[df.c>50]