from itertools import chain

import pandas as pd
import requests
from tqdm import tqdm

from wikidataintegrator.wdi_helpers import id_mapper
from wikidataintegrator.wdi_core import WDItemEngine
from wikidataintegrator import wdi_core, wdi_helpers

PROPS = {'CIViC variant ID': 'P3329',
         'HGVS nomenclature': 'P3331'}

df = pd.read_csv("cgi_biomarkers_per_variant.tsv", sep='\t')

clinical_evidence = {'FDA guidelines', 'European LeukemiaNet guidelines', 'NCCN guidelines', 'CPIC guidelines',
                     'NCCN/CAP guidelines'}
df_clin = df[df['Evidence level'].isin(clinical_evidence)]

# MUT only
gDNA = set(df_clin.dropna(subset=['gDNA']).gDNA)

hgvs_qid = id_mapper(PROPS['HGVS nomenclature'])

{x:hgvs_qid[x] for x in gDNA if x in hgvs_qid}