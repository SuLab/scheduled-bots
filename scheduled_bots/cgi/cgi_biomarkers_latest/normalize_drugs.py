from itertools import chain

import pandas as pd
from tqdm import tqdm

from wikidataintegrator.wdi_helpers import id_mapper
from wikidataintegrator.wdi_core import WDItemEngine

df = pd.read_csv("cgi_biomarkers_per_variant.tsv", sep='\t')

# normalize drug names to QIDs
# first try matching the name to the WHO International Nonproprietary Name (https://www.wikidata.org/wiki/Property:P2275)
name_qid = dict()
whoinn_qid = {k.lower(): v for k, v in id_mapper("P2275").items()}
single_drugs = set(df[(df.Drug.str.count(";") == 0) & (~df.Drug.fillna("").str.startswith("["))].Drug.str.lower())
combo = set(chain(*df[(df.Drug.str.count(";") > 0)].Drug.str.split(";")))
multi = set(chain(*df[df.Drug.fillna("").str.startswith("[")].Drug.str[1:-1].str.split(","))) - {''}
all_drugs = {x.lower() for x in single_drugs | combo | multi}

not_found = [x for x in all_drugs if x.lower() not in whoinn_qid]
name_qid.update({k.lower(): whoinn_qid[k.lower()] for k in all_drugs if k.lower() in whoinn_qid})


# those not found, try to match by string to an item in wikidata that is an instance of chemical compound
# mwapi docs: https://www.mediawiki.org/wiki/Wikidata_query_service/User_Manual/MWAPI
def search_for_drug(drug_name):
    query = """SELECT ?item ?label WHERE {
      SERVICE wikibase:mwapi {
          bd:serviceParam wikibase:api "EntitySearch" .
          bd:serviceParam wikibase:endpoint "www.wikidata.org" .
          bd:serviceParam mwapi:search "***s***" .
          bd:serviceParam mwapi:language "en" .
          ?item wikibase:apiOutputItem mwapi:item .
          ?label wikibase:apiOutputItem mwapi:label
      }
      ?item (wdt:P279|wdt:P31) wd:Q11173 .
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
    }""".replace("***s***", drug_name)
    results = [{k: v['value'].replace("http://www.wikidata.org/entity/", "") for k, v in x.items()} for x in
               WDItemEngine.execute_sparql_query(query)['results']['bindings']]
    return results


not_found2 = []
for drug in tqdm(not_found):
    results = search_for_drug(drug)
    if len(results) == 1:
        name_qid[drug] = results[0]['item']
    else:
        results = [x for x in results if x['label'].lower() == drug]
        if len(results) == 1:
            name_qid[drug] = results[0]['item']
        else:
            not_found2.append(drug)
# manually specify the remaining 26, not including those that aren't actually drugs
# and greg added these as aliases to the wikidata page
manual = {'4ohtestosterone': 'Q4637157',
          'ag-120': 'Q18881245',
          # 'anthracyclines': '',
          'ar42': 'Q27276699',
          'azd5363': 'Q27074756',
          'azd6738': 'Q27896182',
          # 'bcl2 inhibitor': '',
          'byl719': 'Q27074391',
          # 'chemotherapy': '',
          # 'chk1/2 inhibitor': '',
          'entrictinib': 'Q25323953',  # spelled wrong
          'flourouracil': 'Q238512',  # spelled wrong
          'fluvestrant': 'Q5508491',  # spelled wrong
          'gdc-0810': 'Q27272746',
          'hm61713': 'Q27088175',
          # 'hsp90 inhibitor': '',
          # 'lhrh analogues or antagonist': '',
          'liposomal doxorubicin': 'Q29004943',  # pharmaceutical product
          # 'mek inhibitor': '',
          'mercaptopurine': 'Q418529',
          'mk2206': 'Q25100065',
          'mytomycin c': 'Q19856779',
          'octreotide': 'Q27088142',  # 'Q419935' chiral version
          'orterone': 'Q6581305',  # spelled wrong
          # 'platinum agent': '',
          'tensirolimus': 'Q7699074'  # spelled wrong
          }
name_qid.update(manual)

# combination drugs
# example: https://www.wikidata.org/wiki/Q7697766
combo = set(df[(df.Drug.str.count(";") > 0)].Drug)
combo = set([x for x in combo if all(y.lower() in name_qid for y in x.split(";"))])
# will have to make these combinations in wikidata

# multiple drugs listed
multi = set(df[(df.Drug.str.startswith("[")) & (df.Drug.str.len() > 2)].Drug)
multi_map = {m.lower(): {name_qid.get(x.lower()) for x in m[1:-1].split(",")} for m in multi}
multi_map = {k:v for k,v in multi_map.items() if all(x is not None for x in v)}
multi_map = {k: ";".join(v) for k,v in multi_map.items()}
name_qid.update(multi_map)
# why are some in brackets but only one drug is listed?? (e.g.: [Crizotinib])

## drug families
# get families where there are no drugs listed
families = set(df[df.Drug.isin({'[]', ''})]['Drug family'].str.replace("[", "").str.replace("]", ""))
family_map = {'BRAF inhibitor': 'CHEBI:75047',
              }
# there's a lot. What about just the ones where the evidence clinical guielines?
clinical_evidence = {'FDA guidelines', 'European LeukemiaNet guidelines', 'NCCN guidelines', 'CPIC guidelines',
                     'NCCN/CAP guidelines'}
df_clin = df[df['Evidence level'].isin(clinical_evidence)]
families = set(df_clin[df_clin.Drug.isin({'[]', ''})]['Drug family'].str.replace("[", "").str.replace("]", ""))
# 'EGFR inhibitor 1st gen': there's CHEBI:74440 for EGFR inhibitors, but don't know how to specify gen.
# Skip. only two...

df['Drug_qid'] = df.Drug.str.lower().map(name_qid.get)
df.to_csv("cgi_biomarkers_per_variant.tsv", sep="\t", index=None)
