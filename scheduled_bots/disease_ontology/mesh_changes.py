"""
Instead of the change detector looking at each revision for an item
what i want here, is to compare the current state of an item's key/value pairs that I define, with another
set of data (a reference dataset, from an owl/obographs json file)

Steps:
- Does a sparql query against wikidata to get all mesh IDs on all items with a DOID. Looks for a mapping relation type (P4390)
  if available. If no mapping rel type is specified, default to oboInOwl:hasDbXref
- Sparql query against the latest doid.owl release file looking for mesh terms using the relations:
  {skos:closeMatch skos:narrowMatch skos:broadMatch skos:relatedMatch skos:exactMatch oboInOwl:hasDbXref}
- Compare the mesh IDs on wd vs whats in DO. Returns a table listing all of the differences

"""
import subprocess
from collections import defaultdict

import pandas as pd
import requests
from rdflib import Graph
from rdflib import URIRef, Literal
from tqdm import tqdm

from wikidataintegrator.wdi_core import WDItemEngine
from wikidataintegrator.wdi_helpers import id_mapper

BIOPORTAL_KEY = "a1ac23bb-23cb-44cf-bf5e-bcdd7446ef37"
DOID_QID = id_mapper("P699")
DO_OWL_PATH = "doid.owl"
QID_MAP_REL_TYPE_CURIE = {'Q39893184': 'skos:closeMatch',
                          'Q39893967': 'skos:narrowMatch',
                          'Q39894595': 'skos:broadMatch',
                          'Q39894604': 'skos:relatedMatch',
                          'Q39893449': 'skos:exactMatch'}
QID_MAP_REL_TYPE_CURIE = defaultdict(lambda: "oboInOwl:hasDbXref", QID_MAP_REL_TYPE_CURIE)
"""
MAP_REL_TYPE_QID = {'http://www.w3.org/2004/02/skos/core#broadMatch': 'Q39894595',
                    'http://www.w3.org/2004/02/skos/core#closeMatch': 'Q39893184',
                    'http://www.w3.org/2004/02/skos/core#exactMatch': 'Q39893449',
                    'http://www.w3.org/2004/02/skos/core#narrowMatch': 'Q39893967',
                    'http://www.w3.org/2004/02/skos/core#relatedMatch': 'Q39894604'}
"""
PREFIX_TO_CURIE = {
    'http://www.w3.org/2004/02/skos/core#': 'skos',
    'http://www.geneontology.org/formats/oboInOwl#': 'oboInOwl'
}

purl_to_curie = lambda s: s.replace("http://purl.obolibrary.org/obo/", "").replace("_", ":")
curie_to_purl = lambda s: "http://purl.obolibrary.org/obo/" + s.replace(":", "_")


def get_wikidata_do_mesh():
    # get mesh xrefs, and including mapping relation type
    # {'DOID:0050856': {'skos:broadMatch_D019958'}}
    query = """
    select ?item ?doid ?mesh ?mesh_rt where {
      ?item wdt:P699 ?doid .
      ?item p:P486 ?mesh_s .
      ?mesh_s ps:P486 ?mesh .
      optional { ?mesh_s pq:P4390 ?mesh_rt }
    }"""
    results = WDItemEngine.execute_sparql_query(query)['results']['bindings']
    results = [{k: v['value'].replace("http://www.wikidata.org/entity/", "") for k, v in item.items()} for item in
               results]

    df = pd.DataFrame(results)
    df['mesh_rt'] = df.apply(lambda row: QID_MAP_REL_TYPE_CURIE[row.mesh_rt] + "_MESH:" + row.mesh, axis=1)

    df['_item'] = df['item']
    r = df.groupby("_item").aggregate(lambda x: set(y for y in x if not pd.isnull(y))).to_dict("records")
    wd = {list(x['doid'])[0]: x for x in r}
    wd = {k: v['mesh_rt'] for k, v in wd.items()}
    wd = {k: v for k, v in wd.items() if v}
    return wd


def getConceptLabel(qid):
    return getConceptLabels((qid,))[qid]


def getConceptLabels(qids):
    qids = "|".join({qid.replace("wd:", "") if qid.startswith("wd:") else qid for qid in qids})
    params = {'action': 'wbgetentities', 'ids': qids, 'languages': 'en', 'format': 'json', 'props': 'labels'}
    r = requests.get("https://www.wikidata.org/w/api.php", params=params)
    print(r.url)
    r.raise_for_status()
    wd = r.json()['entities']
    return {k: v['labels']['en']['value'] for k, v in wd.items()}


def get_do_metadata():
    # from the do owl file, get do labels, descriptions
    g = Graph()
    g.parse(DO_OWL_PATH)

    disease_ontology = Literal('disease_ontology', datatype=URIRef('http://www.w3.org/2001/XMLSchema#string'))
    query = """
        SELECT * WHERE {
            ?id oboInOwl:hasOBONamespace ?disease_ontology .
            ?id rdfs:label ?label .
            OPTIONAL {?id obo:IAO_0000115 ?descr}
            FILTER NOT EXISTS {?id owl:deprecated ?dep}
        }
        """
    rows = g.query(query, initBindings={'disease_ontology': disease_ontology})
    res = [{str(k): str(v) for k, v in binding.items()} for binding in rows.bindings]
    df = pd.DataFrame(res)
    df.drop_duplicates(subset=['id'], inplace=True)
    df.fillna("", inplace=True)
    do = df.to_dict("records")
    do = {purl_to_curie(x['id']): x for x in do}
    return do


def parse_do_owl():
    """
    Parse xrefs and skos matches from owl file.
    Returns dict. key: doid curie, value: set of xrefs in the format: relation type + "_" + xref. (ex: oboInOwl:hasDbXref_MESH:D007690)
    :return:
    """
    g = Graph()
    g.parse(DO_OWL_PATH)

    disease_ontology = Literal('disease_ontology', datatype=URIRef('http://www.w3.org/2001/XMLSchema#string'))
    true = Literal('true', datatype=URIRef('http://www.w3.org/2001/XMLSchema#boolean'))
    query = """
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    SELECT ?id ?rel_type ?xref WHERE {
        ?id oboInOwl:hasOBONamespace ?disease_ontology .
        OPTIONAL {
          values ?rel_type {skos:closeMatch skos:narrowMatch skos:broadMatch skos:relatedMatch skos:exactMatch oboInOwl:hasDbXref}
          ?id ?rel_type ?xref .
        }
        FILTER NOT EXISTS {?id owl:deprecated ?true}
    }
    """
    rows = g.query(query, initBindings={'disease_ontology': disease_ontology, 'true': true})
    res = [{str(k): str(v) for k, v in binding.items()} for binding in rows.bindings]
    df = pd.DataFrame(res)
    df["doid"] = df["id"]
    df.dropna(subset=['xref'], inplace=True)
    df.rel_type = df.rel_type.apply(
        lambda x: x.replace(x.split("#")[0] + "#", PREFIX_TO_CURIE[x.split("#")[0] + "#"] + ":"))
    df.xref = df.apply(lambda row: row.rel_type + "_" + row.xref, axis=1)

    r = df.groupby("id").aggregate(lambda x: set(y for y in x if not pd.isnull(y))).to_dict("records")
    do = {purl_to_curie(list(x['doid'])[0]): x for x in r}
    do = {k: v['xref'] for k, v in do.items()}
    # filter mesh xrefs only
    do = {k: set([x for x in v if "MESH:" in x]) for k, v in do.items()}
    do = {k: v for k, v in do.items() if v}
    # do['DOID:5570']
    return do


def compare(wd, do):
    # for each DO item, does wd have everything it should? What else does it have?
    wd = defaultdict(set, wd)
    do = defaultdict(set, do)
    leftover_in_wd = dict()
    leftover_in_do = dict()
    doids = set(wd.keys()) | set(do.keys())
    missing = []
    for doid in doids:
        leftover_in_wd[doid] = set()
        leftover_in_do[doid] = set()
        if doid not in wd:
            missing.append(doid)
            continue
        leftover_in_wd[doid] = wd[doid] - do[doid]
        leftover_in_do[doid] = do[doid] - wd[doid]

    leftover_in_wd = {k: v for k, v in leftover_in_wd.items() if v}
    leftover_in_do = {k: v for k, v in leftover_in_do.items() if v}
    print("Items missing in wikidata: {}".format(missing))
    return leftover_in_wd, leftover_in_do


def get_changes():
    wd = get_wikidata_do_mesh()
    do = parse_do_owl()
    leftover_in_wd, leftover_in_do = compare(wd, do)
    return leftover_in_wd, leftover_in_do


def get_mesh_info(mesh_id):
    url = "http://data.bioontology.org/ontologies/MESH/classes/http%3A%2F%2Fpurl.bioontology.org%2Fontology%2FMESH%2F{}"
    d = requests.get(url.format(mesh_id), params={'apikey': BIOPORTAL_KEY}).json()
    if "errors" in d:
        return {'mesh_label': '', 'mesh_descr': ''}
    d = {'mesh_label': d['prefLabel'], 'mesh_descr': d['definition'], 'mesh_synonyms': ";".join(d['synonym'])}
    d['mesh_descr'] = d['mesh_descr'][0] if d['mesh_descr'] else ''
    return d


def get_mesh_changes(leftover_in_wd):
    # from the things added to wikidata, make a table with the metadata about the change
    # starting with things added to wd

    mesh_info = []
    mesh_url = "https://meshb.nlm.nih.gov/record/ui?ui={}"
    do_metadata = get_do_metadata()
    for doid, meshs in tqdm(leftover_in_wd.items()):
        for mesh in meshs:
            relation, mesh = mesh.split("_")
            mesh = mesh.split(":")[1]
            qid = DOID_QID[doid]
            do_node = do_metadata.get(doid, dict())
            x = {'qid': qid, 'wd_label': getConceptLabel(qid),
                 'doid': doid, 'do_label': do_node.get("label"), 'doid_url': curie_to_purl(doid),
                 'do_def': do_node.get("descr"),
                 'mesh': mesh, 'mesh_url': mesh_url.format(mesh),
                 'relation': relation}
            x.update(get_mesh_info(mesh))
            mesh_info.append(x)
    df = pd.DataFrame(mesh_info)
    df = df[['doid', 'do_label', 'do_def', 'doid_url', 'mesh', 'mesh_label',
             'mesh_descr', 'mesh_url', 'qid', 'wd_label', 'relation']]
    print(df.head(2))

    remove_me = df[df.mesh_label.isnull()]
    if not remove_me.empty:
        print("you should remove these")
        print(remove_me)

    # make a formatted df
    df_fmt = df.copy()
    df_fmt.doid = df_fmt.apply(lambda x: "[" + x.doid + "](" + x.doid_url + ")", 1)
    del df_fmt['doid_url']
    df_fmt.mesh = df_fmt.apply(lambda x: "[" + x.mesh + "](" + x.mesh_url + ")", 1)
    del df_fmt['mesh_url']
    df_fmt.qid = df_fmt.qid.apply(lambda x: "[" + x + "](https://www.wikidata.org/wiki/" + x + ")")

    return df, df_fmt


def download_do_owl(release):
    url = "https://github.com/DiseaseOntology/HumanDiseaseOntology/raw/master/src/ontology/releases/{}/doid.owl"
    subprocess.check_call(["wget", "-N", url.format(release)])


def main(release):
    # release = "2017-11-28"
    download_do_owl(release)
    leftover_in_wd, leftover_in_do = get_changes()
    df, df_fmt = get_mesh_changes(leftover_in_wd)
    return df, df_fmt
