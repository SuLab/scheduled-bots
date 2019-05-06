"""
Get the all Xrefs on all disease items in Wikidata
Using an owl file (from DO), get all xrefs
Figure out what the differences are
Generate a ROBOT file to implement all changes
"""
import os
from collections import defaultdict, Counter
from functools import reduce

import requests
from itertools import chain
import csv
import pandas as pd
from rdflib import Graph
from rdflib import URIRef, Literal
from tqdm import tqdm

from ../../scheduled_bots.disease_ontology.robot.utils import get_mesh_info, get_gard_info, get_ordo_info, get_omim_info
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


def get_doid_qid_map():
    # get a good QID to DOID map, using exact match only
    query = """
    SELECT distinct ?disease ?doid ?mrt WHERE {
      ?disease p:P699 ?s_doid .
      ?s_doid ps:P699 ?doid .
      OPTIONAL {?s_doid pq:P4390 ?mrt} .
    }
    """
    df = WDItemEngine.execute_sparql_query(query, as_dataframe=True)
    df.disease = df.disease.str.replace("http://www.wikidata.org/entity/", "")
    df = df[df.mrt.isnull() | (df.mrt == "http://www.wikidata.org/entity/Q39893449")]
    df.drop_duplicates(subset=['disease', 'doid'], inplace=True)
    # make sure one doid goes with one qid
    bad1 = df[df.duplicated("disease", keep=False)]
    bad2 = df[df.duplicated("doid", keep=False)]
    # get rid of these baddies
    df = df[~df.index.isin(bad1.index)]
    df = df[~df.index.isin(bad2.index)]
    doid_qid = dict(zip(df.doid, df.disease))

    return doid_qid


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

    # get xrefs on items where DO is an exact match, and the statement doesn't have a MONDO reference
    # getting ones in which it has no reference, or it has a reference which is not from mondo
    # is too complicated, so get the ref info and we'll filter out the mondo ones afterwards
    query_template = """
    SELECT ?disease ?doid ?xref ?match ?mondo WHERE {{
      ?disease p:P699 ?s_doid .
      ?s_doid ps:P699 ?doid .
      OPTIONAL {{ ?s_doid pq:P4390 ?match}}
      ?disease p:{xref_pid} ?s_xref .
      ?s_xref ps:{xref_pid} ?xref .
      OPTIONAL {{?s_xref prov:wasDerivedFrom ?ref .
                       ?ref pr:P5270 ?mondo }}
    }}
    """

    # these match the prefixes in DO (but uppercase)
    xref_pid = {
        'omim': 'P492',
        'icd9cm': 'P1692',
        'icd10cm': 'P4229',
        'ordo': 'P1550',
        'gard': 'P4317',
        'mesh': 'P486',
        'UMLS_CUI': 'P2892',
        'nci': 'P1748',
    }

    dfs = []
    for xref, pid in xref_pid.items():
        query = query_template.format(xref_pid=pid)
        df = WDItemEngine.execute_sparql_query(query, as_dataframe=True)
        # has no reference but if it does, is not a mondo reference
        df = df[df.mondo.isnull()]
        # has no qualifier or is an exact match
        df = df[df.match.isnull() | (df.match == "http://www.wikidata.org/entity/Q39893449")]
        df = df.groupby('doid').xref.apply(lambda x: ",".join(x)).reset_index().set_index("doid")
        df.rename(columns={'xref': xref.upper()}, inplace=True)
        dfs.append(df)

    # join all of these dfs together
    dfj = reduce(lambda x, y: x.join(y), dfs)

    d = dict()
    for doid, row in dfj.iterrows():
        s = set()
        for k, v in row.to_dict().items():
            if not pd.isnull(v):
                s.update(k + ":" + vv for vv in v.split(","))
        d[doid] = {'xref': s}

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
    # print("Items missing in wikidata: {}".format(missing_in_wd))
    # print("Items missing in DO: {}".format(missing_in_do))
    for k in doids:
        # get rid of the OMIM PS ids
        wd[k]['xref'] = {x for x in wd[k]['xref'] if not x.startswith("OMIM:PS")}
        do[k]['xref'] = {x for x in do[k]['xref'] if not x.startswith("OMIM:PS")}
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


def run_gard(wd, do, leftover_in_wd):
    records = []
    url = "https://rarediseases.info.nih.gov/diseases/{}/index"
    for k, v in leftover_in_wd.items():
        records.extend([{'doid': k, 'doid_label': do[k]['label'],
                         'gard_id': x, 'gard_url': url.format(x)} for x in v if x.startswith("GARD:")])
    for record in tqdm(records):
        record.update(get_gard_info(record['gard_id'].split(":")[1]))
    # classify match by exact sring match on title or one of the aliases
    for record in tqdm(records):
        if record['doid_label'].lower() == record['gard_label'].lower():
            record['match_type'] = 'exact'
        elif record['doid_label'].lower() in map(str.lower, record['gard_synonyms'].split(";")):
            record['match_type'] = 'exact_syn'
        else:
            record['match_type'] = 'not'
    # rename keys
    records = [{k.replace("gard", "ext"): v for k, v in record.items()} for record in records]
    records = sorted(records, key=lambda x: x['doid'])

    make_robot_xref_additions_records(wd, do, [x for x in records if x['match_type'] in {'exact', 'exact_syn'}],
                                      "gard xref additions exact.csv")
    make_robot_xref_additions_records(wd, do, [x for x in records if x['match_type'] == 'not'],
                                      "gard xref additions notexact.csv")


def run_ordo(wd, do, leftover_in_wd):
    records = []
    url = "http://www.orpha.net/consor/cgi-bin/OC_Exp.php?Lng=EN&Expert={}"
    for k, v in leftover_in_wd.items():
        records.extend([{'doid': k, 'doid_label': do[k]['label'], 'ordo_id': x,
                         'ordo_url': url.format(x.replace("ORDO:", ""))} for x in v if x.startswith("ORDO:")])
    # get ordo labels
    for record in tqdm(records):
        record.update(get_ordo_info(record['ordo_id'].replace("ORDO:", "")))
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
    records = sorted(records, key=lambda x: x['doid'])

    make_robot_xref_additions_records(wd, do, [x for x in records if x['match_type'] == 'exact'],
                                      "ordo xref additions exact.csv")
    make_robot_xref_additions_records(wd, do, [x for x in records if x['match_type'] == 'exact_syn'],
                                      "ordo xref additions exact_syn.csv")
    make_robot_xref_additions_records(wd, do, [x for x in records if x['match_type'] == 'not'],
                                      "ordo xref additions notexact.csv")


def run_omim(wd, do, leftover_in_wd):
    records = []
    url = "https://omim.org/entry/{}"
    for k, v in leftover_in_wd.items():
        records.extend([{'doid': k, 'doid_label': do[k]['label'], 'omim_id': x,
                         'omim_url': url.format(x.replace("OMIM:", ""))} for x in v if x.startswith("OMIM:")])
    # get ordo labels
    for record in tqdm(records):
        record.update(get_omim_info(record['omim_id'].replace("OMIM:", "")))
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
    records = sorted(records, key=lambda x: x['doid'])

    make_robot_xref_additions_records(wd, do, [x for x in records if x['match_type'] == 'exact'],
                                      "omim xref additions exact.csv")
    make_robot_xref_additions_records(wd, do, [x for x in records if x['match_type'] == 'exact_syn'],
                                      "omim xref additions exact_syn.csv")
    make_robot_xref_additions_records(wd, do, [x for x in records if x['match_type'] == 'not'],
                                      "omim xref additions notexact.csv")


def run():
    doid_qid = get_doid_qid_map()
    wd = get_wikidata_do_xrefs()
    for k, v in wd.items():
        v['disease'] = doid_qid.get(k)

    wd = {k: v for k, v in wd.items() if v['disease']}
    do = parse_do_owl()

    leftover_in_wd, leftover_in_do = compare(wd, do)
    # print(leftover_in_do)

    # counts of what we have
    Counter([x.split(":")[0] for x in chain(*leftover_in_wd.values())])
    """
    Counter({'GARD': 218,
         'MESH': 1008,
         'NCI': 518,
         'OMIM': 474,
         'ORDO': 1946,
         'UMLS_CUI': 2953})
    """

    run_mesh(wd, do, leftover_in_wd)
    run_gard(wd, do, leftover_in_wd)
    run_ordo(wd, do, leftover_in_wd)
    run_omim(wd, do, leftover_in_wd)


if __name__ == "__main__":
    run()
