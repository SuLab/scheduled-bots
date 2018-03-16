"""
Get the all Xrefs on all disease items in Wikidata
Using an owl file (from DO), get all xrefs
Figure out what the differences are
Generate a ROBOT file to implement all changes
"""
import os
from collections import defaultdict
from itertools import chain
import csv
import pandas as pd
from rdflib import Graph
from rdflib import URIRef, Literal

from wikidataintegrator.wdi_core import WDItemEngine

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
    results = WDItemEngine.execute_sparql_query(query)['results']['bindings']
    results = [{k: v['value'].replace("http://www.wikidata.org/entity/", "") for k, v in item.items()} for item in
               results]

    df = pd.DataFrame(results)
    assert len(df.doid) == len(set(df.doid))

    r = df.to_dict("records")
    r = [{k[:-1] if k.endswith("G") else k: set(v.split(",")) if k.endswith("G") and v else v for k, v in x.items()} for
         x in r]
    r = [{k: v for k, v in x.items() if v} for x in r]
    d = {x['doid']: x for x in r}
    return d


def parse_xrefs(xrefs):
    # turns a list of xref curies into a dict: {'prefix': {'set of values'}, ... }
    d = defaultdict(set)
    for xref in xrefs:
        prefix, value = xref.split(":", 1)
        prefix = prefix.strip()
        value = value.strip()
        if prefix in PREFIXES:
            d[prefix].add(value)
    d = dict(d)
    return d


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
    # df.dropna(subset=['xref'], inplace=True)
    df = df.merge(df.groupby("id").xref.apply(lambda x: set(x.dropna())).reset_index(), on="id", how="outer")
    del df['xref_x']
    df.rename(columns={'xref_y': 'xref'}, inplace=True)
    df.drop_duplicates("id", inplace=True)
    r = df.to_dict("records")
    do = {x['doid']: x for x in r}
    for k, v in do.items():
        v.update(parse_xrefs(v['xref']))
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
    for doid in doids:
        leftover_in_wd[doid] = dict()
        leftover_in_do[doid] = dict()
        for prefix in PREFIXES:
            leftover_in_wd[doid][prefix] = wd[doid].get(prefix, set()) - do[doid].get(prefix, set())
            leftover_in_do[doid][prefix] = do[doid].get(prefix, set()) - wd[doid].get(prefix, set())
        # get rid of the OMIM PS ids
        leftover_in_do[doid]['OMIM'] = {x for x in leftover_in_do[doid]['OMIM'] if not x.startswith("PS")}
        # get rid of GARD because we didn't add those
        del leftover_in_do[doid]['GARD']
        # remove icd9 and 10 because something is weird with a couple of those
        del leftover_in_do[doid]['ICD10CM']
        del leftover_in_do[doid]['ICD9CM']

        leftover_in_wd[doid] = {k: v for k, v in leftover_in_wd[doid].items() if v}
        leftover_in_do[doid] = {k: v for k, v in leftover_in_do[doid].items() if v}

    leftover_in_wd = {k: v for k, v in leftover_in_wd.items() if v}
    leftover_in_do = {k: v for k, v in leftover_in_do.items() if v}

    return leftover_in_wd, leftover_in_do


first_line = ["ID", "Label", "Class Type", "DbXref", "QID"]
second_line = ["ID", "A rdfs:label", "CLASS_TYPE", "A oboInOwl:hasDbXref SPLIT=|", ""]


def make_robot_xref_additions(do, wd, leftover_in_wd):
    # make robot file
    # robot template docs: https://github.com/ontodev/robot/blob/master/docs/template.md
    with open("xref additions.csv", 'w') as f:
        # f = open("test.csv", 'w')
        w = csv.writer(f)
        w.writerow(first_line)
        w.writerow(second_line)
        for ID in leftover_in_wd:
            # ID = "DOID:0060330"
            Label = do[ID]['label']
            xrefs = set(chain(*[[":".join([k, vv]) for vv in v] for k, v in leftover_in_wd[ID].items()]))
            DbXref = "|".join(xrefs)
            class_type = "equivalent"
            qid = wd[ID]['disease']
            qid_url = "https://www.wikidata.org/wiki/{}".format(qid)
            line = [ID, Label, class_type, DbXref, qid_url]
            w.writerow(line)


def make_robot_xref_subtractions(do, wd, leftover_in_do):
    # make robot file
    # robot template docs: https://github.com/ontodev/robot/blob/master/docs/template.md
    with open("xref subs.csv", 'w') as f:
        # f = open("test.csv", 'w')
        w = csv.writer(f)
        w.writerow(first_line)
        w.writerow(second_line)
        for ID in leftover_in_do:
            # ID = "DOID:0060330"
            Label = do[ID]['label']
            xrefs = set(chain(*[[":".join([k, vv]) for vv in v] for k, v in leftover_in_do[ID].items()]))
            DbXref = "|".join(xrefs)
            class_type = "equivalent"
            qid = wd[ID]['doid']
            qid_url = "https://www.wikidata.org/wiki/{}".format(qid)
            line = [ID, Label, class_type, DbXref, qid_url]
            w.writerow(line)


def get_latest_owl():
    # get the latest release
    URL = 'http://purl.obolibrary.org/obo/doid.owl'
    os.system("wget -N {}".format(URL))


wd = get_wikidata_do_xrefs()
do = parse_do_owl()

leftover_in_wd, leftover_in_do = compare(wd, do)
print(leftover_in_do)

make_robot_xref_additions(do, wd, leftover_in_wd)
make_robot_xref_subtractions(do, wd, leftover_in_do)
