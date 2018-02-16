"""
Run download_openfda.sh first
This file expects openfda.json.gz in same dir

This parses and filters the openfda data into a pandas df


"""

import pandas as pd
import json
import gzip
import os
import requests
import pickle
from tqdm import tqdm
from functools import lru_cache
from collections import Counter
from itertools import chain

pd.set_option("display.max_columns", 30)

from wikidataintegrator import wdi_helpers

# load in everything
with gzip.open("openfda.json.gz", 'rt', encoding='utf8') as f:
    f = map(lambda x: json.loads(x), f)
    ds = list(f)

# make sure all docs have all keys
keys = set(chain(*[d.keys() for d in ds]))
for d in ds:
    for key in keys:
        if key not in d:
            d[key] = []

# make these values single values (and uppercase), not lists
single_value_keys = {'manufacturer_name', 'spl_id', 'spl_set_id', 'brand_name', 'application_number', 'generic_name',
                     'product_type'}
for key in single_value_keys:
    for d in ds:
        d[key] = d[key][0].upper() if d[key] else d[key]
# uppercase other things too
for d in ds:
    d['substance_name'] = [x.strip().upper() for x in d['substance_name']]

# filter out some things we aren't going to use
# human prescription drug, is a nda, has atleast one unii and rxcui
print(len(ds))
ds = [d for d in ds if d['product_type'] == 'HUMAN PRESCRIPTION DRUG']
print(len(ds))
ds = [d for d in ds if "NDA" in d['application_number']]
print(len(ds))
ds = [d for d in ds if d['unii'] and d['rxcui']]
print(len(ds))

# toss those where the brand_name == generic_name
# these are generics, don't need them
ds = [d for d in ds if d['brand_name'] != d['generic_name']]
print(len(ds))

# toss those in which the number of unii ids doesn't match the number of substances
ds = [d for d in ds if len(d['substance_name']) == len(d['unii'])]
print(len(ds))

# some brand names have different UNIIs.. for some reason ?
df = pd.DataFrame(ds)
bn_unii = df.groupby("brand_name").agg({'unii': lambda x: set(frozenset(y) for y in x)})
bn_unii = bn_unii[bn_unii.unii.apply(len) > 1]
bn_unii = dict(zip(bn_unii.index, bn_unii.unii))
print(len(bn_unii))
# todo: these should be looked at and fixed ?
# as an example, YUVAFEM in one record contains ESTRADIOL, and in one contains ESTRADIOL HEMIHYDRATE
# df.loc[df.brand_name == "YUVAFEM", ['application_number','brand_name', 'generic_name', 'substance_name', 'unii']]
# df.loc[df.brand_name == "ACTONEL", ['application_number','brand_name', 'generic_name', 'substance_name', 'unii']]
df = df[~df.brand_name.isin(bn_unii.keys())]

ds = df.to_dict("records")

with open("openfda_filtered.json", "w") as f:
    json.dump(ds, f, indent=2)

df.to_csv("openfda_filtered.csv")