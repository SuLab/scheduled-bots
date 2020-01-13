## Bot for adding Prop65 ID

from wikidataintegrator import wdi_core, wdi_login, wdi_helpers
from wikidataintegrator.ref_handlers import update_retrieved_if_new_multiple_refs
import pandas as pd
from pandas import read_csv
import requests
import time
from datetime import datetime
import copy


## Here are the object QIDs, assuming that a chemical is the subject
object_qid = {'femrep':'Q55427776',
              'menrep': 'Q55427774',
              'devtox': 'Q72941151',
              'cancer': 'Q187661',
              'reptox': 'Q55427767'}

list_date = {'femrep':'Female Reproductive Toxicity - Date of Listing',
             'menrep':'Male Reproductive Toxicity - Date of Listing',
             'devtox':'Male Reproductive Toxicity - Date of Listing',
             'cancer': 'None',
             'reptox': 'None'}

list_prop = "P31" 



def create_reference(prop65_url):
    refStatedIn = wdi_core.WDItemID(value="Q28455381", prop_nr="P248", is_reference=True)
    timeStringNow = datetime.now().strftime("+%Y-%m-%dT00:00:00Z")
    refRetrieved = wdi_core.WDTime(timeStringNow, prop_nr="P813", is_reference=True)
    refURL = wdi_core.WDUrl(value=prop65_url, prop_nr="P854", is_reference=True)
    return [refStatedIn, refRetrieved, refURL]



def check_statement_status(wd_item,object_qid):
    new_statetypes = []
    new_dep_types = []
    dep_no_change = []
    no_change=[]
    rank_delist = []
    rank_relist = []
    for object_type in object_qid.keys():
        if eachitem in prop_65_mapped['WDID'].loc[prop_65_mapped[object_type+' delisted']==True].tolist():
            if eachitem in deprecated_df['WDID'].loc[deprecated_df['deprecated_type']==object_type+' delisted'].tolist():
                dep_no_change.append(object_type)
            elif eachitem in current_df['WDID'].loc[current_df['ObjectType']==object_type+' current'].tolist():
                rank_delist.append(object_type)
            else:
                new_dep_types.append(object_type)
        if eachitem in prop_65_mapped['WDID'].loc[prop_65_mapped[object_type+' current']==True].tolist():
            if eachitem in deprecated_df['WDID'].loc[deprecated_df['deprecated_type']==object_type+' delisted'].tolist():
                rank_relist.append(object_type)
            elif eachitem in current_df['WDID'].loc[current_df['ObjectType']==object_type+' current'].tolist():
                no_change.append(object_type)
            else:
                new_statetypes.append(object_type)    
    comparison_dict = {'new_statetypes':new_statetypes, 
                       'new_dep_types':new_dep_types, 
                       'dep_no_change':dep_no_change, 
                       'no_change':no_change, 
                       'rank_delist':rank_delist, 
                       'rank_relist':rank_relist}   
    return(comparison_dict)



#### This is just some logic for handling reptox vs femrep, menrep and devtox
def rep_redundancy_check (repcheck, comparison_dict):
    if (((len(repcheck.intersection(set(comparison_dict['new_statetypes']))) >= 1) or
        (len(repcheck.intersection(set(comparison_dict['no_change']))) >= 1)) and 
        ('reptox' in comparison_dict['new_statetypes'])):
        comparison_dict['new_statetypes'].remove('reptox')
    if (((len(repcheck.intersection(set(comparison_dict['new_dep_types']))) >= 1) or 
        (len(repcheck.intersection(set(comparison_dict['dep_no_change']))) >= 1)) and 
        ('reptox' in comparison_dict['new_dep_types'])):
        comparison_dict['new_dep_types'].remove('reptox')    
    if len(repcheck.intersection(set(comparison_dict['rank_delist']))) >= 1 and 'reptox' in comparison_dict['rank_delist']:
        comparison_dict['rank_delist'].remove('reptox')  
    if len(repcheck.intersection(set(comparison_dict['rank_relist']))) >= 1 and 'reptox' in comparison_dict['rank_relist']:
        comparison_dict['rank_relist'].remove('reptox')   
    return(comparison_dict)



#### Include statement on why it's deprecated if it's deprecated
def generate_statements(statetype_set,dep_list,eachitem_row):
    statements_to_add = [] 
    for j in range(len(statetype_set)):
        run_type = statetype_set[j]
        run_object_wdid = object_qid[run_type]
        date_type = list_date[run_type]
        qualifier_list = []
        if date_type != 'None':
            runlist_date = str(eachitem_row.iloc[0][date_type])
            if runlist_date != 'None':
                list_qualifier = wdi_core.WDTime(datetime.strptime(runlist_date,'%m/%d/%Y').strftime("+%Y-%m-%dT00:00:00Z"), prop_nr='P580', is_qualifier=True)
                qualifier_list.append(list_qualifier)
        if run_type in dep_list:
            qualifier_list.append(delist_reason)
            state_rank = 'deprecated'
        else:
            state_rank = 'normal'
        prop65_statement = wdi_core.WDItemID(value=run_object_wdid, prop_nr=list_prop, rank=state_rank,
                                             qualifiers = qualifier_list, references=[copy.deepcopy(reference)])
        statements_to_add.append(prop65_statement)    
        j=j+1
        return(statements_to_add)



## Retrieve previous statements that need to be changed
def retrieve_prev_state_list(subject_qid,dep_object_qid_list):
    wd_item = wdi_core.WDItemEngine(wd_item_id=subject_qid)
    mass_statement = wd_item.get_wd_json_representation()['claims'][list_prop]
    states_to_keep = []
    for i in range(len(mass_statement)):
        sub_qid = mass_statement[i]['mainsnak']['datavalue']['value']['id']
        state_rank = mass_statement[i]['rank']
        qualifier_list = mass_statement[i]['qualifiers']
        reference = mass_statement[i]['references']
        if sub_qid in dep_object_qid_list:
            continue
        else:
            saved_statement = wdi_core.WDItemID(value=sub_qid, prop_nr=list_prop, rank=state_rank,
                                            qualifiers = qualifier_list, references=[copy.deepcopy(reference)])
            states_to_keep.append(saved_statement)
    return(states_to_keep)



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
        


## Add Prop 65 CA IDs relations from CA OEHHA chemicals list Wikidata
prop65_chems = read_csv('data/prop65_chems.tsv',delimiter='\t',header=0, encoding='utf-8', index_col=0)


## Run sparql query to pull all entities with Prop 65 ID (Read Only Run)
sparqlQuery = "SELECT ?item ?CA65 WHERE {?item wdt:P7524 ?CA65}"
result = wdi_core.WDItemEngine.execute_sparql_query(sparqlQuery)
CA65_in_wd_list = []
i=0
while i < len(result["results"]["bindings"]):
    CA65_id = result["results"]["bindings"][i]["CA65"]["value"]
    wdid = result["results"]["bindings"][i]["item"]["value"].replace("http://www.wikidata.org/entity/", "")
    CA65_in_wd_list.append({'WDID':wdid,'url_stub':CA65_id})
    i=i+1
CA65_in_wd = pd.DataFrame(CA65_in_wd_list)


## Account for bad urls
bad_urls = read_csv('data/bad_urls.tsv',delimiter='\t',header=0, encoding='utf-8',index_col=0)
bad_CA65_in_wd = CA65_in_wd.loc[CA65_in_wd['WDID'].isin(bad_urls['WDID'].tolist())]
bad_url_stubs = bad_CA65_in_wd.merge(bad_urls, on='WDID')
bad_CA65_in_wd = CA65_in_wd.loc[CA65_in_wd['WDID'].isin(bad_urls['WDID'].tolist())]
prop65_chems['url_stub'].loc[prop65_chems['CAS Number'].isin(bad_url_stubs['CAS Number'])] = bad_url_stubs['url_stub'].values


## Perform left merge for currently listed and partially delisted items
prop_65_mapped = prop65_chems.merge(CA65_in_wd, on='url_stub', how='left')
prop_65_mapped['devtox current'] = prop_65_mapped['Chemical listed under Proposition 65 as causing'].str.contains("Development")
prop_65_mapped['menrep current'] = prop_65_mapped['Chemical listed under Proposition 65 as causing'].str.contains("Male")
prop_65_mapped['femrep current'] = prop_65_mapped['Chemical listed under Proposition 65 as causing'].str.contains("Female")
prop_65_mapped['cancer current'] = prop_65_mapped['Cancer'].str.contains("Current")
prop_65_mapped['reptox current'] = prop_65_mapped['Reproductive Toxicity'].str.contains("Current")
prop_65_mapped['cancer delisted'] = prop_65_mapped['Cancer'].str.contains("Formerly")
prop_65_mapped['reptox delisted'] = prop_65_mapped['Reproductive Toxicity'].str.contains("Formerly")
prop_65_mapped.loc[(((prop_65_mapped['Developmental Toxicity - Date of Listing']!="None")|
        (prop_65_mapped['Developmental Toxicity - Listing Mechanism']!="None"))&
        (prop_65_mapped['devtox current']==False)), 'devtox delisted'] = True
prop_65_mapped.loc[(((prop_65_mapped['Female Reproductive Toxicity - Date of Listing']!="None")|
        (prop_65_mapped['Female Reproductive Toxicity - Listing Mechanism']!="None"))&
        (prop_65_mapped['femrep current']==False)), 'femrep delisted'] = True
prop_65_mapped.loc[(((prop_65_mapped['Male Reproductive Toxicity - Date of Listing']!="None")|
        (prop_65_mapped['Male Reproductive Toxicity - Listing Mechanism']!="None"))&
        (prop_65_mapped['menrep current']==False)), 'menrep delisted'] = True
prop_65_mapped.fillna(False, inplace=True)


deprecated_results = []
current_results = []

for object_type in object_qid.keys():
    deprecated_query = "SELECT ?item {?item ps:P31 wd:"+object_qid[object_type]+". ?item pq:P2241 wd:Q56478729. }"
    depresult = wdi_core.WDItemEngine.execute_sparql_query(deprecated_query)
    i=0
    while i < len(depresult["results"]["bindings"]):
        wdi_uri = depresult["results"]["bindings"][i]["item"]["value"].replace("http://www.wikidata.org/entity/statement/", "")
        tmp = wdi_uri.split('-')
        WDID = tmp[0]
        deprecated_results.append({'WDID':WDID,'deprecated_type':object_type+' delisted'})
        i=i+1
    
    sparqlQuery = "SELECT ?item WHERE {?item wdt:P31 wd:"+object_qid[object_type]+".}"
    result = wdi_core.WDItemEngine.execute_sparql_query(sparqlQuery)
    j=0
    while j < len(result["results"]["bindings"]):
        wdid = result["results"]["bindings"][j]["item"]["value"].replace("http://www.wikidata.org/entity/", "")
        current_results.append({'WDID':wdid,'ObjectType':object_type+' current'})
        j=j+1    

        
deprecated_df = pd.DataFrame(deprecated_results)
current_df = pd.DataFrame(current_results)



## all prop_65_CA WDIDs
all_entities = prop_65_mapped['WDID'].tolist()



## Main function
delist_reason = wdi_core.WDItemID('Q56478729', prop_nr='P2241', is_qualifier=True)


edit_log = []
repcheck = set(['femrep','menrep','devrep'])
for eachitem in all_entities:
    eachitem_row = prop_65_mapped.loc[prop_65_mapped['WDID']==eachitem]
    prop65_url = url_base+ eachitem_row.iloc[0]['url_stub']
    reference = create_reference(prop65_url)
    comparison_dict = check_statement_status(eachitem,object_qid)
    comparison_dict = rep_redundancy_check(repcheck,comparison_dict)
    dep_states = comparison_dict['rank_delist']
    change_states = comparison_dict['rank_delist']+comparison_dict['rank_relist']
    if len(change_states)>0:
        change_object_qid_list = []
        for eachhaz in change_states:
            change_object_qid_list.append(object_qid[eachhaz])
        states_to_keep = retrieve_prev_state_list(eachitem,change_object_qid_list)     
        changes_to_add = generate_statements(change_states,dep_states,eachitem_row)
        states_to_write = states_to_keep+changes_to_add 
        item = wdi_core.WDItemEngine(wd_item_id=prop_65_qid, data=states_to_write, append_value=None,
                               global_ref_mode='CUSTOM', ref_handler=update_retrieved_if_new_multiple_refs)
        item.write(login, edit_summary="added CA prop 65 relation info")
               
    dep_list = comparison_dict['new_dep_types']
    statetype_set = comparison_dict['new_statetypes']+dep_list
    if len(statetype_set) > 0:
        statements_to_add = generate_statements(statetype_set,dep_list,eachitem_row)
        item = wdi_core.WDItemEngine(wd_item_id=prop_65_qid, data=statements_to_add, append_value=list_prop,
                               global_ref_mode='CUSTOM', ref_handler=update_retrieved_if_new_multiple_refs)
        item.write(login, edit_summary="added CA prop 65 relation info")
        
    edit_log.append({'WDID':eachitem,'new statements':len(statetype_set),
                     'no_change':(len(comparison_dict['dep_no_change'])+len(comparison_dict['no_change'])),
                     'rank_changes':len(change_states)})        
        
        
edit_log_df = pd.DataFrame(edit_log)
edit_log_df.to_csv('data/log_'+str(datetime.now().strftime("+%Y-%m-%dT00:00:00Z"))+'.tsv',sep='\t',header=True)