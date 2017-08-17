from itertools import chain

import pandas as pd
import requests
from tqdm import tqdm

from wikidataintegrator.wdi_helpers import id_mapper
from wikidataintegrator.wdi_core import WDItemEngine

df = pd.read_csv("cgi_biomarkers_per_variant.tsv", sep='\t')

clinical_evidence = {'FDA guidelines', 'European LeukemiaNet guidelines', 'NCCN guidelines', 'CPIC guidelines',
                     'NCCN/CAP guidelines'}
df_clin = df[df['Evidence level'].isin(clinical_evidence)]

# MUT only
gDNA = set(df_clin.dropna(subset=['gDNA']).gDNA)

# get hgvs to civic IDs
url = "http://myvariant.info/v1/query?q=_exists_:civic&fields=civic&size=1000"
hgvs_civic_map = {x['_id']: x['civic']['variant_id'] for x in requests.get(url).json()['hits']}
url = "http://myvariant.info/v1/query?q=_exists_:civic&fields=civic&size=1000&from=1000"
hgvs_civic_map.update({x['_id']: x['civic']['variant_id'] for x in requests.get(url).json()['hits']})

{k:hgvs_civic_map.get(k) for k in gDNA}