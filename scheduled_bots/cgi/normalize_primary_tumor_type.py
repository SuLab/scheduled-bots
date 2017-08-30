from itertools import chain

import pandas as pd

df = pd.read_csv("cgi_biomarkers_per_variant.tsv", sep='\t')

clinical_evidence = {'FDA guidelines', 'European LeukemiaNet guidelines', 'NCCN guidelines', 'CPIC guidelines',
                     'NCCN/CAP guidelines'}
df_clin = df[df['Evidence level'].isin(clinical_evidence)]
ptts = set(chain(*[x.split(";") for x in set(df_clin['Primary Tumor type'])]))

prim_tt = {'Acute lymphoblastic leukemia': 'http://purl.obolibrary.org/obo/DOID_9952',
           'Acute myeloid leukemia': 'http://purl.obolibrary.org/obo/DOID_9119',
           'Acute promyelocytic leukemia': 'http://purl.obolibrary.org/obo/DOID_0060318',
           'Any cancer type': 'http://purl.obolibrary.org/obo/DOID_162',
           'Basal cell carcinoma': 'http://purl.obolibrary.org/obo/DOID_2513',
           'Breast': 'http://purl.obolibrary.org/obo/DOID_1612',
           'Chronic myeloid leukemia': 'http://purl.obolibrary.org/obo/DOID_8552',
           'Colorectal adenocarcinoma': 'http://purl.obolibrary.org/obo/DOID_0050861',
           'Cutaneous melanoma': 'http://purl.obolibrary.org/obo/DOID_8923',
           'Eosinophilic chronic leukemia': '',
           'Erdheim-Chester histiocytosis': 'http://purl.obolibrary.org/obo/DOID_4329',
           'Gastroesophageal junction adenocarcinoma': 'http://purl.obolibrary.org/obo/DOID_4944',
           'Gastrointestinal stromal': 'http://purl.obolibrary.org/obo/DOID_9253',
           'Giant cell astrocytoma': 'http://purl.obolibrary.org/obo/DOID_5077',
           'Hyper eosinophilic advanced snydrome': 'http://purl.obolibrary.org/obo/DOID_999',
           'Lagerhans cell histiocytosis': 'http://purl.obolibrary.org/obo/DOID_2571',
           'Lung': 'http://purl.obolibrary.org/obo/DOID_1324',
           'Lung adenocarcinoma': 'http://purl.obolibrary.org/obo/DOID_3910',
           'Medulloblastoma': 'http://purl.obolibrary.org/obo/DOID_0050902',
           'Myelodisplasic proliferative syndrome': 'http://purl.obolibrary.org/obo/DOID_4972',
           'Myelodisplasic syndrome': 'http://purl.obolibrary.org/obo/DOID_0050908',
           'Myelofibrosis': 'http://purl.obolibrary.org/obo/DOID_4971',
           'Non-small cell lung': 'http://purl.obolibrary.org/obo/DOID_3908',
           'Ovary': 'http://purl.obolibrary.org/obo/DOID_2394',
           'Renal angiomyolipoma': 'http://purl.obolibrary.org/obo/DOID_8411',
           'Stomach': 'http://purl.obolibrary.org/obo/DOID_10534',
           'Systemic mastocytosis': 'http://purl.obolibrary.org/obo/DOID_349',
           'Thyroid carcinoma': 'http://purl.obolibrary.org/obo/DOID_3963'}

#"\n".join(["|".join([k,"[{}]({})".format(v,v)]) for k,v in prim_tt.items()])


from wikidataintegrator.wdi_helpers import id_mapper
doid_qid = id_mapper("P699")
cgi_tt_qid = {k: doid_qid.get(v.replace("http://purl.obolibrary.org/obo/", "").replace("_",":")) for k,v in prim_tt.items()}

df['prim_tt_qid'] = df['Primary Tumor type'].map(cgi_tt_qid.get)
df.to_csv("cgi_biomarkers_per_variant.tsv", sep="\t", index=None)
