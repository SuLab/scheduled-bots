"""
Get the all Xrefs on all disease items in Wikidata
Using an owl file (from DO), get all xrefs
Figure out what the differences are
Generate a ROBOT file to implement all changes
"""
import os
from collections import defaultdict, Counter
import requests
from itertools import chain
import csv
import pandas as pd
from rdflib import Graph
from rdflib import URIRef, Literal
from tqdm import tqdm
from functools import lru_cache

from wikidataintegrator.wdi_core import WDItemEngine
from wikicurie import wikicurie

cu = wikicurie.CurieUtil()

BIOPORTAL_KEY = "a1ac23bb-23cb-44cf-bf5e-bcdd7446ef37"
DO_OWL_PATH = "doid.owl"

purl_to_curie = lambda s: s.replace("http://purl.obolibrary.org/obo/", "").replace("_", ":")
curie_to_purl = lambda s: "http://purl.obolibrary.org/obo/" + s.replace(":", "_")
PREFIX_TO_CURIE = {
    'http://www.w3.org/2004/02/skos/core#': 'skos',
    'http://www.geneontology.org/formats/oboInOwl#': 'oboInOwl'
}

PREFIXES = {'UMLS_CUI', 'ORDO', 'OMIM', 'NCI', 'MESH', 'ICD9CM', 'ICD10CM', 'GARD'}


def get_wikidata_do_xrefs():
    """
    From wikidata, get all items with a DOID on them. Get all of the following external-ids (defined in PREFIXES)
    :return: dict. values look like:
    'DOID:8499': {'ICD10CM': {'H53.6', 'H53.60'},
      'ICD9CM': {'368.6', '368.60'},
      'MESH': {'D009755'},
      'NCI': {'C34850', 'C37997'},
      'UMLS_CUI': {'C0028077'},
      'disease': 'Q7758678',
      'doid': 'DOID:8499'}
    """
    query = """
    SELECT ?disease ?doid ?GARDG ?OMIMG ?ICD9CMG ?ICD10CMG ?MESHG ?ORDOG ?UMLS_CUIG ?NCIG WHERE {
      {
        SELECT ?disease ?doid
          (group_concat(distinct ?gard; separator=",") as ?GARDG)
          (group_concat(distinct ?omim; separator=",") as ?OMIMG)
          (group_concat(distinct ?icd9cm; separator=",") as ?ICD9CMG)
          (group_concat(distinct ?icd10cm; separator=",") as ?ICD10CMG)
          (group_concat(distinct ?mesh; separator=",") as ?MESHG)
          (group_concat(distinct ?orphanet; separator=",") as ?ORDOG)
          (group_concat(distinct ?umls; separator=",") as ?UMLS_CUIG)
          (group_concat(distinct ?nci; separator=",") as ?NCIG)  WHERE {
            ?disease wdt:P699 ?doid .
            OPTIONAL {?disease wdt:P492 ?omim}
            OPTIONAL {?disease wdt:P1692 ?icd9cm}
            OPTIONAL {?disease wdt:P4229 ?icd10cm}
            OPTIONAL {?disease wdt:P1550 ?orphanet}
            OPTIONAL {?disease wdt:P4317 ?gard}
            OPTIONAL {?disease wdt:P486 ?mesh}
            OPTIONAL {?disease wdt:P2892 ?umls}
            OPTIONAL {?disease wdt:P1748 ?nci}
         } GROUP BY ?disease ?doid
      }
    } """
    df = WDItemEngine.execute_sparql_query(query, as_dataframe=True)
    df.disease = df.disease.str.replace("http://www.wikidata.org/entity/", "")
    assert len(df.doid) == len(set(df.doid)), "{}".format(df[df.doid.duplicated(keep=False)])

    rs = df.to_dict("records")
    newrs = []
    for r in rs:
        s = set()
        newr = dict()
        for k, v in r.items():
            if k.endswith("G") and v:
                s.update(k[:-1] + ":" + vv for vv in v.split(","))
            elif not k.endswith("G"):
                newr[k] = v
        newr['xref'] = s
        newrs.append(newr)

    d = {x['doid']: x for x in newrs}
    return d


def get_duplicated_xrefs(d):
    # from the output of get_wikidata_do_xrefs
    # check if some are duplicated
    xref_count = defaultdict(set)
    for doid, dd in d.items():
        for xref in dd['xref']:
            xref_count[xref].add(doid)
    xref_count = {k: vv for k, vv in xref_count.items() if len(vv) > 1}
    return xref_count


def parse_do_owl():
    """
    Parse xrefs from owl file.
    :return
     'DOID:0110719': {'descr': 'A Warburg micro syndrome that has_material_basi .. ',
        'doid': 'DOID:0110719',
        'id': 'http://purl.obolibrary.org/obo/DOID_0110719',
        'label': 'Warburg micro syndrome 4',
        'xref': {'ICD10CM': {'Q87.0'}, 'OMIM': {'615663'}}}
    """
    g = Graph()
    g.parse(DO_OWL_PATH)

    true = Literal('true', datatype=URIRef('http://www.w3.org/2001/XMLSchema#boolean'))
    query = """
    SELECT ?id ?doid ?descr ?label ?xref WHERE {
        # ?id oboInOwl:id ?doid .  # not all have this for some reason....
        ?id rdfs:label ?label .
        OPTIONAL { ?id obo:IAO_0000115 ?descr }
        OPTIONAL { ?id oboInOwl:hasDbXref ?xref }
        FILTER NOT EXISTS {?id owl:deprecated ?true}
    }
    """
    rows = g.query(query, initBindings={'true': true})
    res = [{str(k): str(v).strip() for k, v in binding.items()} for binding in rows.bindings]

    df = pd.DataFrame(res)
    df["doid"] = df.id.map(purl_to_curie)
    df = df[df.doid.str.startswith("DOID:")]
    df.xref = df.xref.replace("", pd.np.nan)
    df.descr = df.descr.fillna("")
    df = df.groupby(["id", "label", "descr", "doid"]).xref.apply(lambda x: set(x.dropna())).reset_index()
    r = df.to_dict("records")
    do = {x['doid']: x for x in r}
    return do


def compare(wd, do):
    # for each DO item, does wd have everything it should? What else does it have?
    leftover_in_wd = dict()
    leftover_in_do = dict()
    doids = set(wd.keys()) & set(do.keys())
    missing_in_do = set(wd.keys()) - set(do.keys())
    missing_in_wd = set(do.keys()) - set(wd.keys())
    print("Items missing in wikidata: {}".format(missing_in_wd))
    print("Items missing in DO: {}".format(missing_in_do))
    for k in doids:
        # get rid of the OMIM PS ids
        wd[k]['xref'] = {x for x in wd[k]['xref'] if not x.startswith("OMIM:PS")}
        do[k]['xref'] = {x for x in do[k]['xref'] if not x.startswith("OMIM:PS")}
        # get rid of GARD because we didn't add those
        wd[k]['xref'] = {x for x in wd[k]['xref'] if not x.startswith("GARD")}
        do[k]['xref'] = {x for x in do[k]['xref'] if not x.startswith("GARD")}
        # remove icd9 and 10 for now
        wd[k]['xref'] = {x for x in wd[k]['xref'] if not x.startswith("ICD")}
        do[k]['xref'] = {x for x in do[k]['xref'] if not x.startswith("ICD")}
        # remove snomed because not in wd
        wd[k]['xref'] = {x for x in wd[k]['xref'] if not x.startswith("SNOMED")}
        do[k]['xref'] = {x for x in do[k]['xref'] if not x.startswith("SNOMED")}
        # replace NCI2004_11_17:C5453 with NCI
        do[k]['xref'] = {x.replace("NCI2004_11_17:", "NCI:") for x in do[k]['xref']}

    for doid in doids:
        leftover_in_wd[doid] = wd[doid].get('xref', set()) - do[doid].get('xref', set())
        leftover_in_do[doid] = do[doid].get('xref', set()) - wd[doid].get('xref', set())

    leftover_in_wd = {k: v for k, v in leftover_in_wd.items() if v}
    leftover_in_do = {k: v for k, v in leftover_in_do.items() if v}

    return leftover_in_wd, leftover_in_do


def make_robot_xref_additions_records(wd, do, records, filename):
    # make robot file
    # robot template docs: https://github.com/ontodev/robot/blob/master/docs/template.md
    # records is a list of dicts with keys: 'doid', 'doid_label', 'ext_descr', 'ext_id', 'ext_label', 'ext_synonyms'
    first_line = ["ID", "Label", "Class Type", "DbXref", "QID", "ext_label", "ext_descr", "ext_synonyms", "ext_url",
                  "also_on_do", "also_on_wd"]
    second_line = ["ID", "A rdfs:label", "CLASS_TYPE", "A oboInOwl:hasDbXref SPLIT=|", "", "", "", "", "", ""]
    dupe_xrefs_wd = get_duplicated_xrefs(wd)
    dupe_xrefs_do = get_duplicated_xrefs(do)

    with open(filename, 'w') as f:
        # f = open("test.csv", 'w')
        w = csv.writer(f)
        w.writerow(first_line)
        w.writerow(second_line)
        for record in records:
            # ID = "DOID:0060330"
            ID = record['doid']
            Label = record['doid_label']
            DbXref = record['ext_id']
            class_type = "equivalent"
            qid = wd[ID]['disease']
            qid_url = "https://www.wikidata.org/wiki/{}".format(qid)
            namespace, value = DbXref.split(":")
            also_on_wd = ";".join(dupe_xrefs_wd.get(DbXref, set()))
            also_on_do = ";".join(dupe_xrefs_do.get(DbXref, set()))
            line = [ID, Label, class_type, DbXref, qid_url, record['ext_label'],
                    record['ext_descr'], record['ext_synonyms'], record['ext_url'], also_on_do, also_on_wd]
            w.writerow(line)


def get_latest_owl():
    # get the latest release
    URL = 'http://purl.obolibrary.org/obo/doid.owl'
    os.system("wget -N {}".format(URL))


@lru_cache(maxsize=99999)
def get_mesh_info(mesh_id):
    url = "http://data.bioontology.org/ontologies/MESH/classes/http%3A%2F%2Fpurl.bioontology.org%2Fontology%2FMESH%2F{}"
    d = requests.get(url.format(mesh_id), params={'apikey': BIOPORTAL_KEY}).json()
    if "errors" in d:
        return {'mesh_label': '', 'mesh_descr': '', 'mesh_synonyms': ''}
    d = {'mesh_label': d['prefLabel'], 'mesh_descr': d['definition'], 'mesh_synonyms': ";".join(d['synonym'])}
    d['mesh_descr'] = d['mesh_descr'][0] if d['mesh_descr'] else ''
    return d


@lru_cache(maxsize=99999)
def get_ordo_info(mesh_id):
    url = "https://www.ebi.ac.uk/ols/api/ontologies/ordo/terms?iri=http://www.orpha.net/ORDO/Orphanet_{}"
    d = requests.get(url.format(mesh_id)).json()
    try:
        d = d['_embedded']['terms'][0]
        d = {'ordo_label': d['label'], 'ordo_descr': d['description'], 'ordo_synonyms': ";".join(d['synonyms'])}
        d['ordo_descr'] = d['ordo_descr'][0] if d['ordo_descr'] else ''
    except Exception:
        return {'ordo_label': '', 'ordo_descr': '', 'ordo_synonyms': ''}
    return d


@lru_cache(maxsize=99999)
def get_omim_info(omim_id):
    url = "https://api.omim.org/api/entry?mimNumber={}".format(omim_id)
    params = {'apiKey': 'YusepqJtQDuqSPctv6tmVQ', 'format': 'json'}
    try:
        d = requests.get(url, params=params).json()['omim']['entryList'][0]['entry']
    except Exception:
        return {'omim_label': '', 'omim_descr': '', 'omim_synonyms': '', 'omim_prefix': ''}
    print(d)
    d = {'omim_label': d['titles']['preferredTitle'], 'omim_descr': '',
         'omim_synonyms': d['titles']['alternativeTitles'].replace("\n", "") if 'alternativeTitles' in d[
             'titles'] else '',
         'omim_prefix': d.get('prefix', '')}
    return d


def run_mesh(wd, do, leftover_in_wd):
    ## get only the mesh changes
    records = []
    url = "https://meshb.nlm.nih.gov/record/ui?ui={}"
    for k, v in leftover_in_wd.items():
        records.extend([{'doid': k, 'doid_label': do[k]['label'],
                         'mesh_id': x, 'mesh_url': url.format(x)} for x in v if x.startswith("MESH:")])
    # get mesh labels
    for record in tqdm(records):
        record.update(get_mesh_info(record['mesh_id'].split(":")[1]))
    # classify match by exact sring match on title or one of the aliases
    for record in tqdm(records):
        if record['doid_label'].lower() == record['mesh_label'].lower():
            record['match_type'] = 'exact'
        elif record['doid_label'].lower() in map(str.lower, record['mesh_synonyms'].split(";")):
            record['match_type'] = 'exact_syn'
        else:
            record['match_type'] = 'not'
    # rename keys
    records = [{k.replace("mesh", "ext"): v for k, v in record.items()} for record in records]
    records = sorted(records, key=lambda x: x['doid'])

    make_robot_xref_additions_records(wd, do, [x for x in records if x['match_type'] in {'exact', 'exact_syn'}],
                                      "mesh xref additions exact.csv")
    make_robot_xref_additions_records(wd, do, [x for x in records if x['match_type'] == 'not'],
                                      "mesh xref additions notexact.csv")


def run():
    wd = get_wikidata_do_xrefs()
    do = parse_do_owl()

    leftover_in_wd, leftover_in_do = compare(wd, do)
    # print(leftover_in_do)

    run_mesh(wd, do, leftover_in_wd)

    if False:
        ## get only the OMIM changes
        records = []
        url = "https://omim.org/entry/{}"
        for k, v in leftover_in_wd.items():
            if 'OMIM' in v:
                records.extend([{'doid': k, 'doid_label': do[k]['label'],
                                 'omim_id': x, 'omim_url': url.format(x)} for x in v['OMIM']])
        # get omim labels
        for record in tqdm(records):
            record.update(get_omim_info(record['omim_id']))
        # classify match by exact sring match on title or one of the aliases
        for record in tqdm(records):
            if record['doid_label'].lower() == record['omim_label'].lower():
                record['match_type'] = 'exact'
            elif record['doid_label'].lower() in map(str.lower, record['omim_synonyms'].split(";")):
                record['match_type'] = 'exact_syn'
            else:
                record['match_type'] = 'not'
        # rename keys
        records = [{k.replace("omim", "ext"): v for k, v in record.items()} for record in records]
        # curie-ize ext id
        for record in records:
            record['ext_id'] = "OMIM:" + record['ext_id']
            record['ext_descr'] = record['ext_prefix']
        records = sorted(records, key=lambda x: x['doid'])

        make_robot_xref_additions_records(wd, [x for x in records if x['match_type'] == 'exact'],
                                          "omim xref additions exact.csv")
        make_robot_xref_additions_records(wd, [x for x in records if x['match_type'] == 'exact_syn'],
                                          "omim xref additions exact_syn.csv")
        make_robot_xref_additions_records(wd, [x for x in records if x['match_type'] == 'not'],
                                          "omim xref additions notexact.csv")

        ## get only the ORDO changes
        records = []
        url = "http://www.orpha.net/consor/cgi-bin/OC_Exp.php?Lng=EN&Expert={}"
        for k, v in leftover_in_wd.items():
            if 'ORDO' in v:
                records.extend([{'doid': k, 'doid_label': do[k]['label'],
                                 'ordo_id': x, 'ordo_url': url.format(x)} for x in v['ORDO']])
        # get ordo labels
        for record in tqdm(records):
            record.update(get_ordo_info(record['ordo_id']))
        # classify match by exact sring match on title or one of the aliases
        for record in tqdm(records):
            if record['doid_label'].lower() == record['ordo_label'].lower():
                record['match_type'] = 'exact'
            elif record['doid_label'].lower() in map(str.lower, record['ordo_synonyms'].split(";")):
                record['match_type'] = 'exact_syn'
            else:
                record['match_type'] = 'not'
        # rename keys
        records = [{k.replace("ordo", "ext"): v for k, v in record.items()} for record in records]
        # curie-ize ext id
        for record in records:
            record['ext_id'] = "ORDO:" + record['ext_id']
        records = sorted(records, key=lambda x: x['doid'])

        make_robot_xref_additions_records(wd, [x for x in records if x['match_type'] == 'exact'],
                                          "ordo xref additions exact.csv")
        make_robot_xref_additions_records(wd, [x for x in records if x['match_type'] == 'exact_syn'],
                                          "ordo xref additions exact_syn.csv")
        make_robot_xref_additions_records(wd, [x for x in records if x['match_type'] == 'not'],
                                          "ordo xref additions notexact.csv")


if __name__ == "__main__":
    run()
