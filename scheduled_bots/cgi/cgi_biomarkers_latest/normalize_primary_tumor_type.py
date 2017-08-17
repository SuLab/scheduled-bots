import pandas as pd

df = pd.read_csv("cgi_biomarkers_per_variant.tsv", sep='\t')

clinical_evidence = {'FDA guidelines', 'European LeukemiaNet guidelines', 'NCCN guidelines', 'CPIC guidelines',
                     'NCCN/CAP guidelines'}
df_clin = df[df['Evidence level'].isin(clinical_evidence)]

prim_tt = {'Acute myeloid leukemia': 'http://purl.obolibrary.org/obo/DOID_9119',
 'Acute promyelocytic leukemia': 'http://purl.obolibrary.org/obo/DOID_0060318',
 'Any cancer type': 'http://purl.obolibrary.org/obo/DOID_162',
 'Basal cell carcinoma;Medulloblastoma': 'http://purl.obolibrary.org/obo/DOID_2513;http://purl.obolibrary.org/obo/DOID_0050902',
 'Breast': 'http://purl.obolibrary.org/obo/DOID_1612',
 'Chronic myeloid leukemia': 'http://purl.obolibrary.org/obo/DOID_8552',
 'Chronic myeloid leukemia;Acute lymphoblastic leukemia': 'http://purl.obolibrary.org/obo/DOID_8552;http://purl.obolibrary.org/obo/DOID_9952',
 'Colorectal adenocarcinoma': 'http://purl.obolibrary.org/obo/DOID_0050861',
 'Cutaneous melanoma': 'http://purl.obolibrary.org/obo/DOID_8923',
 'Gastrointestinal stromal': 'http://purl.obolibrary.org/obo/DOID_9253',
 'Gastrointestinal stromal;Myelodisplasic syndrome;Myelodisplasic proliferative syndrome;Hyper eosinophilic advanced snydrome;Eosinophilic chronic leukemia;Chronic myeloid leukemia;Acute lymphoblastic leukemia;Systemic mastocytosis': 1,
 'Giant cell astrocytoma': 1,
 'Hyper eosinophilic advanced snydrome;Eosinophilic chronic leukemia': 1,
 'Lung': 'http://purl.obolibrary.org/obo/DOID_1324',
 'Lung adenocarcinoma': 'http://purl.obolibrary.org/obo/DOID_3910',
 'Myelodisplasic syndrome;Myelodisplasic proliferative syndrome': 2,
 'Myelofibrosis': 'http://purl.obolibrary.org/obo/DOID_4971',
 'Non-small cell lung': 'http://purl.obolibrary.org/obo/DOID_3908',
 'Non-small cell lung;Lagerhans cell histiocytosis;Erdheim-Chester histiocytosis': 6,
 'Non-small cell lung;Lung adenocarcinoma': 2,
 'Ovary': 'http://purl.obolibrary.org/obo/DOID_2394',
 'Renal angiomyolipoma': 1,
 'Renal angiomyolipoma;Giant cell astrocytoma': 1,
 'Stomach;Gastroesophageal junction adenocarcinoma': 2,
 'Thyroid carcinoma': 'http://purl.obolibrary.org/obo/DOID_3963'}
