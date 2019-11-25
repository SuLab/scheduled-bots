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



## Add Prop 65 CA IDs from CA OEHHA chemicals list Wikidata
datasrc = 'data/OEHHA-2019-11-1.csv'

chem_list = read_csv(datasrc, encoding = 'unicode_escape', header=0) 

## Pull out only columns of interest for our task
cols_of_interest = chem_list[['Title','CAS Number','Cancer','Cancer - Listing Mechanism',
                          'Reproductive Toxicity','Chemical listed under Proposition 65 as causing',
                          'Developmental Toxicity - Date of Listing','Developmental Toxicity - Listing Mechanism',
                          'Female Reproductive Toxicity - Date of Listing',
                          'Female Reproductive Toxicity - Listing Mechanism',
                          'Male Reproductive Toxicity - Date of Listing',
                          'Male Reproductive Toxicity - Listing Mechanism']]

prop_65_irrelevant = cols_of_interest.loc[(cols_of_interest['Cancer'] == "None") & 
                                          (cols_of_interest['Reproductive Toxicity'] == "None") & 
                                          (cols_of_interest['Chemical listed under Proposition 65 as causing'] == "None")]
non_prop_chems = prop_65_irrelevant['Title'].tolist()
prop65_chems = cols_of_interest.loc[~cols_of_interest['Title'].isin(non_prop_chems)].copy()

## To convert the title to a url stub, lower case it, strip out parenthesis, brackets, and commas, and replace spaces with dashes
prop65_chems['url_stub'] = prop65_chems['Title'].str.lower().str.replace("[","").str.replace("]","").str.replace(",","").str.replace("(","").str.replace(")","").str.strip("]").str.replace(".","").str.replace(" ","-")
prop65_chems.to_csv('data/prop65_chems.tsv',sep='\t',header=True, encoding='utf-8')

mixnmatch_cat = prop65_chems[['url_stub','Title','CAS Number']].copy()
mixnmatch_cat.rename(columns={'url_stub':'Entry ID','Title':'Entry name'}, inplace=True)
mixnmatch_cat['Entry description'] = mixnmatch_cat['Entry name'].astype(str).str.cat(mixnmatch_cat['CAS Number'].astype(str),sep=", CAS Number: ")

sparqlQuery = "SELECT * WHERE {?item wdt:P231 ?CAS}"
result = wdi_core.WDItemEngine.execute_sparql_query(sparqlQuery)
cas_in_wd_list = []

i=0
while i < len(result["results"]["bindings"]):
    cas_id = result["results"]["bindings"][i]["CAS"]["value"]
    wdid = result["results"]["bindings"][i]["item"]["value"].replace("http://www.wikidata.org/entity/", "")
    cas_in_wd_list.append({'WDID':wdid,'CAS Number':cas_id})
    i=i+1

cas_in_wd = pd.DataFrame(cas_in_wd_list)
cas_in_wd.drop_duplicates(subset='CAS Number',keep=False,inplace=True)
cas_in_wd.drop_duplicates(subset='WDID',keep=False,inplace=True)

prop_65_matches = mixnmatch_cat.merge(cas_in_wd,on='CAS Number',how='inner')

## Pull items already mapped to Prop 65 ID
sparqlQuery = "SELECT ?item ?CA65 WHERE {?item wdt:P7524 ?CA65}"
result = wdi_core.WDItemEngine.execute_sparql_query(sparqlQuery)
CA65_in_wd_list = []

i=0
while i < len(result["results"]["bindings"]):
    CA65_id = result["results"]["bindings"][i]["CA65"]["value"]
    wdid = result["results"]["bindings"][i]["item"]["value"].replace("http://www.wikidata.org/entity/", "")
    CA65_in_wd_list.append({'WDID':wdid,'Entry ID':CA65_id})
    i=i+1

CA65_in_wd = pd.DataFrame(CA65_in_wd_list)

## Remove items matched via mix n match from update
prop_65_less_mixnmatch = prop_65_matches.loc[~prop_65_matches['Entry ID'].isin(CA65_in_wd['Entry ID'].tolist())]

prop65_to_add = prop_65_less_mixnmatch
url_base = 'https://oehha.ca.gov/chemicals/'
list_prop = "P7524" 

## Add Prop65 statements if there's a successful mapping
for i in range(len(prop65_to_add)):
    prop_65_qid = prop65_to_add.iloc[i]['WDID']
    prop_65_id = prop65_to_add.iloc[i]['Entry ID']
    prop_65_url = url_base+prop_65_id
    reference = create_reference(prop_65_url)
    prop65_statement = [wdi_core.WDString(value=prop_65_id, prop_nr=list_prop, 
                               references=[copy.deepcopy(reference)])]
    item = wdi_core.WDItemEngine(wd_item_id=prop_65_qid, data=prop65_statement, append_value=list_prop,
                               global_ref_mode='CUSTOM', ref_handler=update_retrieved_if_new_multiple_refs)
    item.write(login, edit_summary="added CA prop 65 id") 
