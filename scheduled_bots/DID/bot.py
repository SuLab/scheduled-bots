from wikidataintegrator import wdi_core, wdi_login
from wikidataintegrator.ref_handlers import update_retrieved_if_new_multiple_refs
import pandas as pd
from pandas import read_csv
import requests
from tqdm import trange, tqdm
from datetime import datetime
import copy



def create_reference():
    refStatedIn = wdi_core.WDItemID(value="Q70116865", prop_nr="P248", is_reference=True)
    timeStringNow = datetime.now().strftime("+%Y-%m-%dT00:00:00Z")
    refRetrieved = wdi_core.WDTime(timeStringNow, prop_nr="P813", is_reference=True)
    
    return [refStatedIn, refRetrieved]



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
        
        
        
## Load clean triples and write to Wikidata
statement_types = ['cause','may_prevent','may_treat']
property_dict = {'cause':'P1542','may_prevent':'P4954','may_treat':'P2175'}
filelocation = 'results/'

drug_qid_column_to_use = 'drug_cas_wdid'
phen_qid_column_to_use = 'phen_cui_wdid'

for each_did_type in statement_types:
    statement_filename = filelocation+each_did_type+'.tsv'
    triples = read_csv(statement_filename,delimiter='\t',header=0)
    triples_clean = triples[[drug_qid_column_to_use,phen_qid_column_to_use]].copy()
    triples_clean.drop_duplicates(keep='first',inplace=True)
    triples_clean.reset_index(inplace=True)
    i=0
    for i in tqdm(range(len(triples_clean))):
        drug_qid = triples_clean.iloc[i][drug_qid_column_to_use]
        phen_qid = triples_clean.iloc[i][phen_qid_column_to_use]
        reference = create_reference()
        print(each_did_type,drug_qid,phen_qid,property_dict[each_did_type])
        statement = [wdi_core.WDItemID(value=phen_qid, prop_nr=property_dict[each_did_type], references=[copy.deepcopy(reference)])]
        item = wdi_core.WDItemEngine(wd_item_id=drug_qid, data=statement, append_value=property_dict[each_did_type],
                           global_ref_mode='CUSTOM', ref_handler=update_retrieved_if_new_multiple_refs)
        item.write(login)
        i=i+1        
