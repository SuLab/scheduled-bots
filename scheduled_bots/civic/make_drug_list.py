from collections import defaultdict

import requests
from tqdm import tqdm
import json

r = requests.get('https://civic.genome.wustl.edu/api/variants?count=999999999')
variants_data = r.json()
records = variants_data['records']

all_data = dict()
for record in tqdm(records):
    variant_id = str(record['id'])
    r = requests.get('https://civic.genome.wustl.edu/api/variants/' + variant_id)
    variant_data = r.json()
    all_data[variant_id] = variant_data

with open("all_variant_data.json", "w") as f:
    json.dump(all_data, f)

def alwayslist(value):
    """If input value if not a list/tuple type, return it as a single value list."""
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return value
    else:
        return [value]

records = defaultdict(set)
for variant_data in all_data.values():
#variant_data = all_data[0]
    evidence_items = [x for x in variant_data['evidence_items']]
    evidence_items = sorted(evidence_items, key=lambda x: x['id'])
    for evidence_item in evidence_items:
        for drug in evidence_item["drugs"]:
            drug_label = drug['name'].lower()
            record = {'drug_label': drug_label, 'pmid': evidence_item['source']['pubmed_id']}
            records[drug_label].add(evidence_item['source']['pubmed_id'])

records = {k: "|".join(v) for k,v in records.items()}
records = [{"drugname": k, "pmids": v} for k,v in records.items()]

import pandas as pd
df = pd.DataFrame(records)
df.to_csv("drugname_pmid_oct_2018.csv")