from wikidataintegrator import wdi_core, wdi_login
from wikidataintegrator.ref_handlers import update_retrieved_if_new_multiple_refs
import pandas as pd
from pandas import read_csv
import requests
import time
from datetime import datetime
import copy


## Login for Scheduled bot
print("Logging in...")
try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")


## Fetch GARD API credentials
print("Fetching GARD API credentials...")
try:
    from scheduled_bots.local import GARDUSER, GARDPASS
except ImportError:
    if "GARDUSER" in os.environ and "GARDPASS" in os.environ:
        GARDUSER = os.environ['GARDUSER']
        GARDPASS = os.environ['GARDPASS']
    else:
        raise ValueError("GARDUSER and GARDPASS must be specified in local.py or as environment variables")


def create_reference(gard_url):
    refStatedIn = wdi_core.WDItemID(value="Q47517289", prop_nr="P248", is_reference=True)
    timeStringNow = datetime.now().strftime("+%Y-%m-%dT00:00:00Z")
    refRetrieved = wdi_core.WDTime(timeStringNow, prop_nr="P813", is_reference=True)
    refURL = wdi_core.WDUrl(value=gard_url, prop_nr="P854", is_reference=True)
    return [refStatedIn, refRetrieved, refURL]


## Fetch data from GARD
header_info = {GARDUSER: GARDPASS}
gard_results = requests.get('https://api.rarediseases.info.nih.gov/api/diseases',
                           headers=header_info)

## Parse GARD data
gard_df = pd.read_json(gard_results.text)
key_of_interest = "mainPropery"  ## Note it's misspelled in GARD, treated as a variable in case it gets fixed

gard_id_list = gard_df['diseaseId'].unique().tolist()
fail_list = []
no_syns = []
no_idens = []
identifier_df = pd.DataFrame(columns=['diseaseId','identifierId','identifierType'])
synonyms_df = pd.DataFrame(columns=['diseaseId','name','source'])

i=0
while i < len(gard_id_list):
    try:
        sample_result = requests.get('https://api.rarediseases.info.nih.gov/api/diseases/'+str(gard_df.iloc[i]['diseaseId']),
                                   headers=header_info)
        json_result = sample_result.json()
        data_of_interest = json_result.get(key_of_interest)
        sourced_syn = data_of_interest.get('synonyms-with-source')
        tmpdict = pd.DataFrame(sourced_syn).fillna("GARD")
        tmpdict['diseaseId'] = gard_df.iloc[i]['diseaseId']
        if len(tmpdict) == 0:
            no_syns.append(gard_df.iloc[i]['diseaseId'])
        else:
            synonyms_df = pd.concat((synonyms_df,tmpdict),ignore_index=True)

        identifier_results = data_of_interest.get('identifiers')
        identifier_dict = pd.DataFrame(identifier_results).fillna("None")
        identifier_dict['diseaseId'] = gard_df.iloc[i]['diseaseId']
        if len(identifier_dict) == 0:
            no_idens.append(gard_df.iloc[i]['diseaseId'])
        else:
            identifier_df = pd.concat((identifier_df,identifier_dict),ignore_index=True)
    
    except:
        fail_list.append(gard_df.iloc[i]['diseaseId'])
    i=i+1


## Pull all WD entities with GARD IDs
sparqlQuery = "SELECT * WHERE {?item wdt:P4317 ?GARD}"
result = wdi_core.WDItemEngine.execute_sparql_query(sparqlQuery)
gard_in_wd_list = []

i=0
while i < len(result["results"]["bindings"]):
    gard_id = result["results"]["bindings"][i]["GARD"]["value"]
    wdid = result["results"]["bindings"][i]["item"]["value"].replace("http://www.wikidata.org/entity/", "")
    gard_in_wd_list.append({'WDID':wdid,'diseaseId':gard_id})
    i=i+1

gard_in_wd = pd.DataFrame(gard_in_wd_list)
print(gard_in_wd.head(n=3))


## Identify GARD diseases not yet in Wikidata
gard_in_wd_id_list = gard_in_wd['diseaseId'].unique().tolist()

gard_not_in_wd = identifier_df.loc[~identifier_df['diseaseId'].isin(gard_in_wd_id_list)]
property_list = gard_not_in_wd['identifierType'].unique().tolist()
prop_id_dict = {'OMIM':'P492', 'ORPHANET':'P1550', 'UMLS':'P2892',
                'SNOMED CT':'P5806', 'ICD 10':'P494', 'NCI Thesaurus':'P1748',
                'ICD 10-CM':'P4229', 'MeSH':'P486'}

## Pull diseases from WD based on misc identifiers
sparql_start = 'SELECT * WHERE {?item wdt:'
sparql_end = '}'

identifier_megalist=[]

for eachidtype in property_list:
    sparqlQuery = sparql_start + prop_id_dict[eachidtype] + ' ?identifierId'+sparql_end
    result = wdi_core.WDItemEngine.execute_sparql_query(sparqlQuery)
    i=0
    while i in < len(result["results"]["bindings"]):
        id_id = result["results"]["bindings"][i]['identifierId']["value"]
        wdid = result["results"]["bindings"][i]["item"]["value"].replace("http://www.wikidata.org/entity/", "")
        identifier_megalist.append({'WDID':wdid,'identifierId':id_id, 'identifierType':eachidtype})
    i=i+1
    time.sleep(2)
        
identifier_megadf = pd.DataFrame(identifier_megalist)

## For each Gard Disease Entry, check for multiple mappings to the same WDID
missing_gard_merge = gard_not_in_wd.merge(identifier_megadf,on=(['identifierId', 'identifierType']), how="inner")
still_missing = gard_not_in_wd.loc[~gard_not_in_wd['diseaseId'].isin(missing_gard_merge['diseaseId'].unique().tolist())]

## Determine the number of identifiers that support a merge
potential_gard = missing_gard_merge.groupby(['diseaseId','WDID']).size().reset_index(name='identifier_count')
mapping_check1 = gard_ids_to_add.groupby('diseaseId').size().reset_index(name='qid_count')
one_to_many = mapping_check1.loc[mapping_check1['qid_count']>1]

mapping_check2 = gard_ids_to_add.groupby('WDID').size().reset_index(name='gardid_count')
many_to_one = mapping_check2.loc[mapping_check2['gardid_count']>1]

gard_mapping_issue_ids = one_to_many['diseaseId'].unique().tolist() + many_to_one['WDID'].unique().tolist()

gard_to_add = potential_gard.loc[~potential_gard['diseaseId'].isin(gard_mapping_issue_ids) & 
                                     ~potential_gard['WDID'].isin(gard_mapping_issue_ids) &
                                     ~potential_gard['diseaseId'].isin(still_missing)]

gard_to_add_full = gard_to_add.merge(gard_df,on='diseaseId',how="left")

gard_to_auto_add = gard_to_add_full.loc[gard_to_add_full['identifier_count']>1]
gard_to_suggest = gard_to_add_full.loc[gard_to_add_full['identifier_count']==1]

## Create Wikidata statements

gard_map_revision_list = []

i=0
while i < len(gard_to_auto_add):
    gard_qid = gard_to_auto_add.iloc[i]['WDID']
    gard_url = gard_to_auto_add.iloc[i]['websiteUrl']
    gard_id = str(gard_to_auto_add.iloc[i]['diseaseId'])
    reference = create_reference(gard_url)
    gard_prop = "P4317" 
    statement = [wdi_core.WDString(value=gard_id, prop_nr=gard_prop, references=[copy.deepcopy(reference)])]
    item = wdi_core.WDItemEngine(wd_item_id=gard_qid, data=statement, append_value=gard_prop,
                               global_ref_mode='CUSTOM', ref_handler=update_retrieved_if_new_multiple_refs)
    item.write(login,edit_summary='added GARD ID')
    gard_map_revision_list.append(item.lastrevid)
    i=i+1



########################################################################

## Identify synonyms to load to Wikidata

## pull aliases for all entries with GARD IDs
sparqlQuery = 'SELECT ?item ?itemLabel ?GARD ?alias WHERE {?item wdt:P4317 ?GARD. OPTIONAL {?item skos:altLabel ?alias FILTER (LANG (?alias) = "en").} SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}'
result = wdi_core.WDItemEngine.execute_sparql_query(sparqlQuery)

## Format the results from the Wikidata query into Pandas DF for easier manipulation

gard_alias_in_wd_list = []

i=0
while i < len(result["results"]["bindings"]):
    gard_id = result["results"]["bindings"][i]["GARD"]["value"]
    wdid = result["results"]["bindings"][i]["item"]["value"].replace("http://www.wikidata.org/entity/", "")
    label = result["results"]["bindings"][i]["itemLabel"]["value"]
    try:
        alias = result["results"]["bindings"][i]["alias"]["value"]
    except:
        alias = "No alias"
    gard_alias_in_wd_list.append({'WDID':wdid,'diseaseId':int(gard_id),'label':label,'alias':alias})
    i=i+1

gard_alias_in_wd = pd.DataFrame(gard_alias_in_wd_list)


## Pull the aliases that are sourced from GARD
gard_alias = synonyms_df.loc[synonyms_df['source']=='GARD']

## Filter the Wikidata GARD Alias table down to just the GARD IDs in GARD alias DF (ie- has allowable synonyms)
gard_wd_limited_df = gard_alias_in_wd.loc[gard_alias_in_wd['diseaseId'].isin(gard_alias['diseaseId'].unique().tolist())]
alias_check_df = gard_alias.merge(gard_wd_limited_df,on='diseaseId',how='inner').copy()

## Check if the GARD synonym matches anything in the corresponding Wikidata label or alias
alias_check_df['label_match?'] = alias_check_df['name'].str.lower()==alias_check_df['label'].str.lower()
alias_check_df['alias_match?'] = alias_check_df['name'].str.lower()==alias_check_df['alias'].str.lower()

## Identify the GARD synonyms that were found in Wikidata (label or aliases) for removal
synonyms_to_drop = alias_check_df['name'].loc[(alias_check_df['label_match?']==True) | 
                                              (alias_check_df['alias_match?']==True)].unique().tolist()

## Filter out GARD entries that were found in Wikidata
synonyms_to_inspect = alias_check_df.loc[~alias_check_df['name'].isin(synonyms_to_drop)]

## Identify the synonyms to add to wikidata as an alias
synonyms_to_add = synonyms_to_inspect.drop_duplicates(subset=['diseaseId','name','source','WDID','label'], keep='first')

## Script to run the synonym updates
gard_alias_revision_list = []

i=0
while i < len(gard_to_auto_add):
    disease_qid = synonyms_to_add.iloc[i]['WDID']
    disease_alias = synonyms_to_add.iloc[i]['name']
    wikidata_item = wdi_core.WDItemEngine(wd_item_id=disease_qid)
    wikidata_item.set_aliases([disease_alias],lang='en',append=True)
    wikidata_item.write(login, edit_summary='added alias from GARD')
    gard_alias_revision_list.append(wikidata_item.lastrevid)
    i=i+1




######################################################################################

## Export the revisions made by the bot and any failures

with open('data/mapping_revisions.txt','w') as outwritelog:
    for eachrevid in gard_map_revision_list:
        outwritelog.write(str(eachrevid)+'\n')

        
with open('data/alias_revisions.txt','w') as aliaslog:
    for eachrevid in gard_alias_revision_list:
        aliaslog.write(str(eachrevid)+'\n')
        
        
with open('data/no_syns.txt','w') as outwrite:
    for eachentry in no_syns:
        outwrite.write(str(eachentry)+'\n')

        
with open('data/no_idens.txt','w') as idenwrite:
    for eachiden in no_idens:
        idenwrite.write(str(eachiden)+'\n')

gard_to_auto_add.to_cav('data/gard_ids_attempted.tsv',sep='\t',inplace=True)
gard_to_suggest.to_csv('data/gard_ids_found_not_attempted.tsv',sep='\t',inplace=True)
        