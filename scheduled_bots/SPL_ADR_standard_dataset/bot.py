from wikidataintegrator import wdi_core, wdi_login, wdi_helpers
from wikidataintegrator.ref_handlers import update_retrieved_if_new_multiple_refs
import pandas as pd
from pandas import read_csv
import requests
from datetime import datetime
import copy

exppath = 'results/'



def disease_search(spl_adr_raw):
    sparqlQuery = "SELECT * WHERE {?topic wdt:P2892 ?CUI}"
    result = wdi_core.WDItemEngine.execute_sparql_query(sparqlQuery)
    wdmap = []
    i=0
    while i < len(result["results"]["bindings"]):
        umls_qid = result["results"]["bindings"][i]["topic"]["value"].replace("http://www.wikidata.org/entity/", "")
        cui_id = result["results"]["bindings"][i]["CUI"]["value"]
        tmpdict = {'UMLS CUI':cui_id,'disease_WDID':umls_qid}
        wdmap.append(tmpdict)
        i=i+1
    wdid_umls_all = pd.DataFrame(wdmap)
    umls_cui_list = spl_adr_raw['UMLS CUI'].unique().tolist()
    wdid_umls_df = wdid_umls_all.loc[wdid_umls_all['UMLS CUI'].isin(umls_cui_list)]
    wdid_umls_df_unique = wdid_umls_df.drop_duplicates(subset='disease_WDID').copy()
    wdid_umls_df_unique.drop_duplicates(subset='UMLS CUI',inplace=True) 
    return wdid_umls_df_unique



def drug_search(drug_list):
    pharm_start = 'SELECT ?item ?itemLabel WHERE {?item wdt:P31 wd:Q28885102; rdfs:label ?itemLabel.  FILTER(CONTAINS(LCASE(?itemLabel), "'
    med_start = 'SELECT ?item ?itemLabel WHERE {?item wdt:P31 wd:Q12140; rdfs:label ?itemLabel. FILTER(CONTAINS(LCASE(?itemLabel), "'
    chem_start = 'SELECT ?item ?itemLabel WHERE {?item wdt:P31 wd:Q11173; rdfs:label ?itemLabel. FILTER(CONTAINS(LCASE(?itemLabel), "'
    query_end = '"@en)).}'
    drug_wdid_list = []
    drug_match_failed = []
    i=0
    while i < len(drug_list):
        query_subject = drug_list[i].lower()
        try:
            sparqlQuery = pharm_start+query_subject+query_end
            result = wdi_core.WDItemEngine.execute_sparql_query(sparqlQuery)
            drug_qid = result["results"]["bindings"][0]["item"]["value"].replace("http://www.wikidata.org/entity/", "")
            drug_label = result["results"]["bindings"][0]["itemLabel"]["value"]
            drug_wdid_list.append({'Drug Name':drug_list[i], 'drug_WDID':drug_qid, 'drug_wd_label':drug_label, 
                                   'instance_of':'pharmaceutical product'})
        except:
            try:
                sparqlQuery = med_start+query_subject+query_end
                result = wdi_core.WDItemEngine.execute_sparql_query(sparqlQuery)
                drug_qid = result["results"]["bindings"][0]["item"]["value"].replace("http://www.wikidata.org/entity/", "")
                drug_label = result["results"]["bindings"][0]["itemLabel"]["value"]
                drug_wdid_list.append({'Drug Name':drug_list[i], 'drug_WDID':drug_qid, 'drug_wd_label':drug_label, 
                                       'instance_of':'medication'})
            except:
                try:
                    sparqlQuery = chem_start+query_subject+query_end
                    result = wdi_core.WDItemEngine.execute_sparql_query(sparqlQuery)
                    drug_qid = result["results"]["bindings"][0]["item"]["value"].replace("http://www.wikidata.org/entity/", "")
                    drug_label = result["results"]["bindings"][0]["itemLabel"]["value"]
                    drug_wdid_list.append({'Drug Name':drug_list[i], 'drug_WDID':drug_qid, 'drug_wd_label':drug_label, 
                                           'instance_of':'chemical'}) 
                except:
                    drug_match_failed.append(drug_list[i])
        i=i+1
    drug_wdid_df = pd.DataFrame(drug_wdid_list)    
    return drug_wdid_df, drug_match_failed



def create_reference(spl_url,source_type):
    timeStringNow = datetime.now().strftime("+%Y-%m-%dT00:00:00Z")
    archived_date = datetime.strptime('9/29/2015','%m/%d/%Y').strftime("+%Y-%m-%dT00:00:00Z")
    refStatedIn = wdi_core.WDItemID(value="Q73670648", prop_nr="P248", is_reference=True)
    refRetrieved = wdi_core.WDTime(timeStringNow, prop_nr="P813", is_reference=True)
    refRetrieved2 = wdi_core.WDTime(archived_date, prop_nr="P2960", is_reference=True)
    refURL = wdi_core.WDUrl(value=spl_url, prop_nr="P854", is_reference=True)
    reftype = wdi_core.WDString(value=source_type, prop_nr="P958", is_reference=True)
    return [refStatedIn, refRetrieved, refRetrieved2, refURL, reftype]



def write_adrs(run_list):
    fda_base_spl_url = 'https://dailymed.nlm.nih.gov/dailymed/drugInfo.cfm?setid='
    wd_revision_list = []
    i=0
    while i < len(run_list):
        drug_qid = run_list.iloc[i]['drug_WDID']
        disease_qid = run_list.iloc[i]['disease_WDID']
        spl_drug_id = run_list.iloc[i]['Drug ID']
        spl_url = fda_base_spl_url+spl_drug_id
        source_type = run_list.iloc[i]['Section Display Name']
        reference = create_reference(spl_url,source_type)
        treat_qualifier = wdi_core.WDItemID(value="Q179661", prop_nr="P1013", is_qualifier=True)
        effect_qualifier = wdi_core.WDItemID(value="Q217690", prop_nr="P1542", is_qualifier=True)        
        statement = [wdi_core.WDItemID(value=drug_qid, prop_nr="P5642", qualifiers=[treat_qualifier, effect_qualifier],
                                       references=[copy.deepcopy(reference)])]
        wikidata_item = wdi_core.WDItemEngine(wd_item_id=disease_qid, data=statement, append_value="P5642",
                               global_ref_mode='CUSTOM', ref_handler=update_retrieved_if_new_multiple_refs)
        wikidata_item.write(login, edit_summary='added ADR relationship from FDA SPLs')  
        wd_revision_list.append({'drug':drug_qid,'disease':disease_qid,'wd_revid':wikidata_item.lastrevid})
        i=i+1
    wd_edit_results = pd.DataFrame(wd_revision_list)
    return wd_edit_results




###### Main Script

#### Determine the source based on the run
with open('data/run_no.txt', 'r') as run_file:
    for line in run_file:
        run_number = int(line.strip())

if run_number ==0:
    datasrc = 'data/FinalReferenceStandard200Labels.csv'
else:
    datasrc = exppath+'qid_missing_not_attempted.tsv

print("run started: ",datetime.now())
spl_adr_raw = read_csv(datasrc, delimiter="|", header=0, dtype={'Index':int,'PT ID':str,'LLT ID':str}).fillna('None')

## Pull QIDS using UMLS CUIS
wdid_umls_df_unique = disease_search(spl_adr_raw)

## Merge the mapping table to the original table
spl_with_disease_wdids = spl_adr_raw.merge(wdid_umls_df_unique, on='UMLS CUI', how='left')

## Pull QIDS using drug labels
if run_number == 0:
    drug_list = spl_with_disease_wdids['Drug Name'].unique().tolist()
else:
    drug_list = []
    with open(exppath+'drug_match_failed.txt','r') as drug_match_failed:
        for line in drug_match_failed:
            drug_list.append(line+'\n')

drug_wdid_df, drug_match_failed = drug_search(drug_list)

## Merge the results to generate the table of entries to write
df_to_write = spl_with_disease_wdids.merge(drug_wdid_df, on='Drug Name',how = 'left')
all_data_available = df_to_write.loc[(~df_to_write['disease_WDID'].isnull()) & 
                                     (~df_to_write['drug_WDID'].isnull())]

not_attempted = df_to_write.loc[(df_to_write['disease_WDID'].isnull()) | 
                                     (df_to_write['drug_WDID'].isnull())]

## Make the writes
run_list = all_data_available
wd_edit_results = write_adrs(run_list)

#### Export the results of the run

## Results of the drug search
drug_wdid_df.to_csv(exppath+'drug_wdid_df.tsv',sep='\t',header=True)

with open(exppath+'drug_match_failed.txt','w') as store_it:
    for eachfailure in drug_match_failed:
        store_it.write(eachfailure+'\n')

## Results of the actual run
wd_edit_results.to_csv(exppath+'run_results.tsv',sep='\t',header=True)

## Failures to attempt in the future
not_attempted.to_csv(exppath+'qid_missing_not_attempted.tsv',sep='\t',header=True)

print("run completed: ",datetime.now())
run_number = run_number + 1

with open('data/run_no.txt', 'w') as run_file:
    run_file.write(str(run_number))