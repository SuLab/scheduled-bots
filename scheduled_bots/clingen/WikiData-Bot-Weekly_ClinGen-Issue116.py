#!/usr/bin/env python
# coding: utf-8

# Scheduled Integration of ClinGen Gene-Disease Validity Data into WikiData

### ClinGen (Clinical Genome Resource) develops curated data of genetic associations 
### CC0 https://clinicalgenome.org/docs/terms-of-use/

### This scheduled bot operates through WDI to integrate ClinGen Gene-Disease Validity Data 
### https://github.com/SuLab/GeneWikiCentral/issues/116 
### https://search.clinicalgenome.org/kb/gene-validity/ 

### Python script contributions, in order: Sabah Ul-Hasan, Andra Waagmeester, Andrew Su, Ginger Tsueng



# Relevant modules and libraries
### Installations by shell 
get_ipython().system("pip install --upgrade pip")
get_ipython().system('pip3 install tqdm')
get_ipython().system('pip3 install termcolor')
get_ipython().system('pip3 install wikidataintegrator')

### Installations by python
from wikidataintegrator import wdi_core, wdi_login 
from wikidataintegrator.ref_handlers import update_retrieved_if_new_multiple_refs
from datetime import datetime
from termcolor import colored 

import pandas as pd
import numpy as np

import os 
import copy 
import time 

# Login for running WDI

print("Logging in...") 

### **Update to ProteinBoxBot
os.environ["WDUSER"] = "username" 
os.environ["WDPASS"] = "password"

### Conditional that outputs error command if not in the local python environment
if "WDUSER" in os.environ and "WDPASS" in os.environ: 
    WDUSER = os.environ['WDUSER']
    WDPASS = os.environ['WDPASS']
else: 
    raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")      

### Sets attributed username and password as 'login'
login = wdi_login.WDLogin(WDUSER, WDPASS) 


# ClinGen gene-disease validity data
### Read as csv
df = pd.read_csv('https://search.clinicalgenome.org/kb/gene-validity.csv', skiprows=6, header=None)  

### Label column headings
df.columns = ['Gene', 'HGNC Gene ID', 'Disease', 'MONDO Disease ID','SOP','Classification','Report Reference URL','Report Date']

### Create time stamp of when downloaded (error if isoformat() used)
timeStringNow = datetime.now().strftime("+%Y-%m-%dT00:00:00Z")

### Create empty columns for output file (ignore warnings)
df['Status'] = "pending" # "Status" column with 'pending' for all cells: 'error' or 'complete' (meaning previously logged within 180 days)
df['Definitive'] = "" # Empty cell to be replaced with 'yes' or 'no' string
df['Gene QID'] = "" # To be replaced with 'absent' or 'multiple'
df['Disease QID'] = "" # To be replaced with 'absent' or 'multiple'


# Create a function for adding references to then be iterated in the loop "create_reference()"
def create_reference(): 
        refStatedIn = wdi_core.WDItemID(value="Q64403342", prop_nr="P248", is_reference=True) 
        timeStringNow = datetime.now().strftime("+%Y-%m-%dT00:00:00Z") 
        refRetrieved = wdi_core.WDTime(timeStringNow, prop_nr="P813", is_reference=True)
        refURL = wdi_core.WDUrl((df.loc[index, 'Report Reference URL']), prop_nr="P854", is_reference=True) 
        return [refStatedIn, refRetrieved, refURL]

# For loop that executes the following through each row of the dataframe

start_time = time.time() # Keep track of how long it takes loop to run

for index, row in df.iterrows(): 
        
    ### Identify the string in the Gene or Disease column for a given row
    HGNC = df.loc[index, 'HGNC Gene ID'].replace("HGNC:", "") 
    MONDO = df.loc[index, 'MONDO Disease ID'].replace("_", ":")
    
    ### SparQL query to search for Gene or Disease in Wikidata based on HGNC ID (P354) or MonDO ID (P5270)
    sparqlQuery_HGNC = "SELECT * WHERE {?gene wdt:P354 \""+HGNC+"\"}" 
    result_HGNC = wdi_core.WDItemEngine.execute_sparql_query(sparqlQuery_HGNC) 
    sparqlQuery_MONDO = "SELECT * WHERE {?disease wdt:P5270 \""+MONDO+"\"}" 
    result_MONDO = wdi_core.WDItemEngine.execute_sparql_query(sparqlQuery_MONDO)
    
    ### Assign resultant length of dictionary for either Gene or Disease (number of Qid)
    HGNC_qlength = len(result_HGNC["results"]["bindings"]) 
    MONDO_qlength = len(result_MONDO["results"]["bindings"])
    
    ### Conditional utilizing length value for output table, accounts for absent/present combos
    if HGNC_qlength == 1:
        HGNC_qid = result_HGNC["results"]["bindings"][0]["gene"]["value"].replace("http://www.wikidata.org/entity/", "")
        df.at[index, 'Gene QID'] = HGNC_qid 
    if HGNC_qlength < 1: 
        df.at[index, 'Status'] = "error" 
        df.at[index, 'Gene QID'] = "absent"  
    if HGNC_qlength > 1: 
        df.at[index, 'Status'] = "error" 
        df.at[index, 'Gene QID'] = "multiple"
        
    if MONDO_qlength == 1:
        MONDO_qid = result_MONDO["results"]["bindings"][0]["disease"]["value"].replace("http://www.wikidata.org/entity/", "") 
        df.at[index, 'Disease QID'] = MONDO_qid  
    if MONDO_qlength < 1: 
        df.at[index, 'Status'] = "error" 
        df.at[index, 'Disease QID'] = "absent" 
    if MONDO_qlength > 1:
        df.at[index, 'Status'] = "error" 
        df.at[index, 'Disease QID'] = "multiple" 
        
    ### Conditional inputs error such that only rows are written for where Classification = 'Definitive'
    if row['Classification']!='Definitive': 
        df.at[index, 'Status'] = "error" 
        df.at[index, 'Definitive'] = "no" 
        continue 
    else: 
        df.at[index, 'Definitive'] = "yes" 
  
    ### Conditional continues to write into WikiData only if 1 Qid for each + Definitive classification 
    if HGNC_qlength == 1 & MONDO_qlength == 1:
        
        ### Call upon create_reference() function created   
        reference = create_reference() 
        
        # Add disease value to gene item page, and gene value to disease item page (symmetry)
        ### Creates 'gene assocation' statement (P2293) whether or not it's already there, and includes the references
        statement_HGNC = [wdi_core.WDItemID(value=MONDO_qid, prop_nr="P2293", references=[copy.deepcopy(reference)])] 
        wikidata_HGNCitem = wdi_core.WDItemEngine(wd_item_id=HGNC_qid, 
                                                  data=statement_HGNC, 
                                                  global_ref_mode='CUSTOM', # parameter that looks within 180 days
                                                  ref_handler=update_retrieved_if_new_multiple_refs, 
                                                  append_value=["P2293"])
        wikidata_HGNCitem.get_wd_json_representation() # Gives json structure that submitted to API, helpful for debugging 
        
        statement_MONDO = [wdi_core.WDItemID(value=HGNC_qid, prop_nr="P2293", references=[copy.deepcopy(reference)])] 
        wikidata_MONDOitem = wdi_core.WDItemEngine(wd_item_id=MONDO_qid, 
                                                   data=statement_MONDO, 
                                                   global_ref_mode='CUSTOM',
                                                   ref_handler=update_retrieved_if_new_multiple_refs, 
                                                   append_value=["P2293"])
        wikidata_MONDOitem.get_wd_json_representation()
        
        ### Write message for combination successfully logged, and enter 'complete' in Status column
        HGNC_name = df.loc[index, 'Gene'] 
        MONDO_name = df.loc[index, 'Disease']
        
        print(colored(HGNC_name,"blue"), "Gene with HGNC ID", 
              colored(HGNC,"blue"), "logged as Qid",
              colored(wikidata_HGNCitem.write(login),"blue"),
              "and")
        print(colored(MONDO_name,"green"), "Disease with MONDO ID", 
              colored(MONDO,"green"), "logged as Qid",
              colored(wikidata_MONDOitem.write(login),"green"))
        
        df.at[index, 'Status'] = "complete" 
        
end_time = time.time() # Captures when loop run ends
print("The total time of this loop is:", end_time - start_time, "seconds, or", (end_time - start_time)/60, "minutes")

# Write output to a .csv file
now = datetime.now()

# Includes hour:minute:second_dd-mm-yyyy time stamp (https://en.wikipedia.org/wiki/ISO_8601)
df.to_csv("ClinGenBot_Status-Output_" + now.isoformat() + ".csv")  # isoformat
