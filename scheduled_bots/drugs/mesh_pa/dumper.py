import pandas as pd
import requests
pd.set_option("display.width", 120)
from wikidataintegrator import wdi_helpers, wdi_core, wdi_login


URL = "http://id.nlm.nih.gov/mesh/sparql"
PREFIX = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>
PREFIX mesh: <http://id.nlm.nih.gov/mesh/>
"""

def sparql_query(query):
    params = {'query': PREFIX + query, 'format': 'JSON', 'limit': 1000, 'offset': 0}
    r = requests.get(URL, params=params)
    res = [{k: v['value'] for k, v in x.items()} for x in r.json()['results']['bindings']]
    while True:
        params['offset'] += 1000
        print(params['offset'])
        r = requests.get(URL, params=params).json()['results']['bindings']
        if not r:
            break
        res.extend([{k: v['value'] for k, v in x.items()} for x in r])
    df = pd.DataFrame(res)
    return df


def get_mesh_tree():
    query = """
SELECT distinct ?mesh ?treeNum ?meshLabel ?note (GROUP_CONCAT(DISTINCT ?alt_label; separator="|") as ?aliases)
FROM <http://id.nlm.nih.gov/mesh> WHERE {
  FILTER(STRSTARTS(STR(?treeNum), "http://id.nlm.nih.gov/mesh/D27."))
      ?mesh meshv:treeNumber ?treeNum .
      ?mesh meshv:active 1 .
      ?mesh rdfs:label ?meshLabel .
      OPTIONAL {?mesh meshv:preferredConcept [meshv:scopeNote ?note]}
      OPTIONAL {?mesh meshv:preferredConcept [meshv:term [ meshv:prefLabel ?alt_label]]}
} GROUP BY ?mesh ?meshLabel ?note ?treeNum
"""
    df = sparql_query(query)
    df.treeNum = df.treeNum.map(lambda x: x.replace("http://id.nlm.nih.gov/mesh/", ""))
    df.mesh = df.mesh.map(lambda x: x.replace("http://id.nlm.nih.gov/mesh/", ""))
    df['topLevel3'] = df.treeNum.map(lambda x: ".".join(x.split(".")[:3]))
    df['topLevel2'] = df.treeNum.map(lambda x: ".".join(x.split(".")[:2]))
    df['topLevel1'] = df.treeNum.map(lambda x: ".".join(x.split(".")[:1]))
    return df


def get_mesh_pa_counts():
    query = """
    SELECT (COUNT(?d) as ?c) ?mesh ?meshLabel FROM <http://id.nlm.nih.gov/mesh> WHERE {
     ?d meshv:pharmacologicalAction ?mesh.
     ?d meshv:active 1 .
     ?mesh rdfs:label ?meshLabel
    } group by ?mesh ?meshLabel
    order by DESC(?c)
    """
    df = sparql_query(query)
    df.mesh = df.mesh.map(lambda x: x.replace("http://id.nlm.nih.gov/mesh/", ""))
    df.c = df.c.map(int)
    return df


def get_drug_pa():
    query = """SELECT * FROM <http://id.nlm.nih.gov/mesh> WHERE {
      ?mesh meshv:pharmacologicalAction ?pa .
      ?mesh meshv:active 1 .
      ?mesh meshv:preferredConcept [meshv:registryNumber ?rn]
    }"""
    df = sparql_query(query)
    df.mesh = df.mesh.map(lambda x: x.replace("http://id.nlm.nih.gov/mesh/", ""))
    df.pa = df.pa.map(lambda x: x.replace("http://id.nlm.nih.gov/mesh/", ""))
    return df


def _scratch():
    mesh_qid = wdi_helpers.id_mapper("P486", return_as_set=True)
    mesh_qid = {k: list(v)[0] for k, v in mesh_qid.items() if len(v) == 1}
    df = get_mesh_tree()
    df['qid'] = df.mesh.map(mesh_qid.get)
    vc = df.topLevel.value_counts()
    df['vc'] = df.topLevel.map(vc)
    df[df.treeNum.isin(df.topLevel[df.vc>5])]


def get_analogs_derivatives():
    """
    SELECT distinct ?a ?al ?d ?dl FROM <http://id.nlm.nih.gov/mesh> WHERE {
      ?a meshv:preferredMappedTo ?m .
      ?m meshv:hasDescriptor ?d .
      ?m meshv:hasQualifier mesh:Q000031 .
      ?d rdfs:label ?dl .
      ?a rdfs:label ?al .
      FILTER(STRSTARTS(STR(?treeNum), "http://id.nlm.nih.gov/mesh/D"))
      ?d meshv:treeNumber ?treeNum .
    }
    """