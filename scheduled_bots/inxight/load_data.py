import base64
import datetime
import json
import pickle
import time
from collections import defaultdict
from itertools import chain

import requests
from tqdm import tqdm

# url = "https://stitcher.ncats.io/api/stitches/v1/9ZOQ3TZI87"
# d = requests.get(url).json()
xref_keys = ['CAS', 'Cas', 'ChEMBL', 'ChemblId', 'CompoundCID', 'CompoundName', 'CompoundSmiles',
             'CompoundUNII', 'DRUG BANK', 'DrugbankId', 'IUPHAR', 'InChIKey', 'Iupac',
             'KeggId', 'MESH', 'NCI_THESAURUS', 'NDF-RT', 'PUBCHEM', 'RXCUI', 'SMILES', 'UNII', 'Unii',
             'drugbank-id', 'smiles', 'unii']
combine_keys = [
    ('UNII', 'unii', 'Unii', 'CompoundUNII'),
    ('CAS', 'Cas'),
    ('DRUGBANK', 'DrugbankId', 'DRUG BANK', 'drugbank-id'),
    ('CHEMBL.COMPOUND', 'ChEMBL', 'ChemblId')
]


def download_stitcher():
    url = "https://stitcher.ncats.io/api/stitches/v1?top=100&skip={}"
    skip = 0
    contents = []
    t = tqdm(total=98244 / 100)
    while True:
        t.update()
        d = requests.get(url.format(skip)).json()
        if not d['contents']:
            break
        contents.extend(d['contents'])
        skip += 100
        time.sleep(1)
    with open("stitcher_dump_{}.pkl".format(datetime.datetime.now().strftime("%Y-%m-%d")), "wb") as f:
        pickle.dump(contents, f)
    return contents


def alwayslist(value):
    """If input value if not a list/tuple type, return it as a single value list."""
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return value
    else:
        return [value]


def organize_one_record(d):
    node_xref = defaultdict(lambda: defaultdict(set))
    for xref_key in xref_keys:
        for node in alwayslist(d['sgroup']['properties'].get(xref_key, [])):
            node_xref[node['node']][xref_key].add(node['value'])
    node_xref = {k: dict(v) for k, v in node_xref.items()}

    # combine these sets of keys together
    # the first will be the prefix for curieutil and is what they get combined into

    for xrefs_kv in node_xref.values():
        for combine_key in combine_keys:
            xrefs_kv[combine_key[0]] = set(chain(*[xrefs_kv.get(k, set()) for k in combine_key]))
            for k in combine_key[1:]:
                if k in xrefs_kv:
                    del xrefs_kv[k]

    # Drug Products
    targets_nv = alwayslist(d['sgroup']['properties'].get('Targets', []))
    conditions_nv = alwayslist(d['sgroup']['properties'].get('Conditions', []))
    for t in targets_nv:
        t['actualvalue'] = json.loads(base64.b64decode(t['value']).decode())
    for t in conditions_nv:
        t['actualvalue'] = json.loads(base64.b64decode(t['value']).decode())

    conditions = defaultdict(list)
    for condition in conditions_nv:
        conditions[condition['node']].append(condition['actualvalue'])
    conditions = dict(conditions)

    records = []
    for k, v in conditions.items():
        if k not in node_xref:
            print("fuck")
        else:
            records.append({'xrefs': node_xref[k], 'conditions': v})

    return records


def organize_data(contents):
    records = []
    for d in tqdm(contents):
        records.extend(organize_one_record(d))

    records = [x for x in records if len(x['xrefs'].get('CHEMBL.COMPOUND', [])) == 1]
    d = {list(x['xrefs']['CHEMBL.COMPOUND'])[0]: x['conditions'] for x in records}

    for key in d:
        d[key] = [x for x in d[key] if x.get('HighestPhase') == "Approved"]
        d[key] = [x for x in d[key] if x.get('TreatmentModality') == "Primary"]
        d[key] = [x for x in d[key] if x.get('isConditionDoImprecise') is False]
        d[key] = [x for x in d[key] if x.get('ConditionDoId') != "Unknown"]
    d = {k: v for k, v in d.items() if v}

    """
    # Counter([x['TreatmentModality'] for x in chain(*d.values())])
    TreatmentModality
    Counter({'Primary': 1974, 'Palliative': 304, 'Preventing': 165, 'Diagnostic': 87, 'Secondary': 56, 'Inactive ingredient': 30})
    isConditionDoImprecise
    Counter({False: 2160, True: 456})
    """
    keys_to_keep = ['ConditionDoId', 'ConditionProductDate', 'FdaUseURI']
    for key in d:
        d[key] = [{k: v for k, v in x.items() if k in keys_to_keep} for x in d[key]]

    for x in chain(*d.values()):
        for k, v in x.items():
            if v == "Unknown":
                x[k] = None
        for k in keys_to_keep:
            x[k] = x.get(k)
        # 2012-12-07
        x['ConditionProductDate'] = datetime.datetime.strptime(x['ConditionProductDate'], '%Y-%m-%d') if \
            x['ConditionProductDate'] else None
        x['ConditionDoId'] = "DOID:" + str(x['ConditionDoId']) if x['ConditionDoId'] else None

    return d


def load_parsed_data():
    d = pickle.load(open("stitcher_parsed_2018-10-23.pkl", 'rb'))
    return d


def main():
    contents = pickle.load(open("stitcher_dump_2018-10-11.pkl", 'rb'))
    d = organize_data(contents)
    with open("stitcher_parsed_{}.pkl".format(datetime.datetime.now().strftime("%Y-%m-%d")), "wb") as f:
        pickle.dump(d, f)
