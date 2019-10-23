from wikidataintegrator import wdi_core, wdi_login, wdi_helpers
from wikidataintegrator.ref_handlers import update_retrieved_if_new_multiple_refs
import pandas as pd
from pandas import read_csv
import requests
from tqdm import trange, tqdm
import xml.etree.ElementTree as et 
import time
from datetime import datetime
import copy

datasrc = 'https://ghr.nlm.nih.gov/download/TopicIndex.xml'



## GHR inheritance codes to WD entities mapping
GHR_WD_codes = {'ac': 'Q13169788', ##wd:Q13169788 (codominant)
               'ad': 'Q116406', ##wd:Q116406 (autosomal dominant)
               'ar': 'Q15729064', ##wd:Q15729064 (autosomal recessive)
               'm': 'Q15729075', ##wd:Q15729075 (mitochondrial)
               'x': 'Q70899378', #wd:Q2597344 (X-linked inheritance)
               'xd': 'Q3731276', ##wd:Q3731276 (X-linked dominant)
               'xr': 'Q1988987', ##wd:Q1988987 (X-linked recessive)
               'y': 'Q2598585'} ##wd:Q2598585 (Y linkage)

GHR_codes_no_WD = {'n': 'not inherited', 'u': 'unknown pattern'}




def create_reference(ghr_url):
    refStatedIn = wdi_core.WDItemID(value="Q62606821", prop_nr="P248", is_reference=True)
    timeStringNow = datetime.now().strftime("+%Y-%m-%dT00:00:00Z")
    refRetrieved = wdi_core.WDTime(timeStringNow, prop_nr="P813", is_reference=True)
    refURL = wdi_core.WDUrl(value=ghr_url, prop_nr="P854", is_reference=True)

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

       
    
## Retrieve topics from topic dump on NLM and parse    
r = requests.get(datasrc)
xml = r.text
xtree = et.fromstring(xml)
topic_of_interest = 'Conditions'

for eachtopic in xtree.findall('topic'):
    if eachtopic.attrib['id'] == topic_of_interest:
        new_tree = eachtopic.find('topics')

conditions = new_tree



## Parse the topics url list
conditions_list = []

for condition in conditions.findall('topic'):
    title = condition.find('title').text
    url = condition.find('url').text
    try:
        synonyms = condition.find('other_names')
        for synonym in synonyms:
            tmpdict = {'title': title,'url':url,'aka':synonym.text}
            conditions_list.append(tmpdict)
    except:
        tmpdict = {'title': title,'url':url,'aka':'None'}
        conditions_list.append(tmpdict)
    
conditions_df = pd.DataFrame(conditions_list)



## Use NLM GHR API to pull xrefs and inheritance data
conditions_url_list = conditions_df['url'].unique().tolist()
condition_url_list_test = conditions_url_list[0:3]

inher_list = []
inher_fail = []
syn_fail = []
synonyms_df = pd.DataFrame(columns = ['topic','synonym'])
xref_list = []
xref_fail = []

u=0
for u in tqdm(range(len(conditions_url_list))):
    eachurl = conditions_url_list[u]
    tmpurl = eachurl+'?report=json'
    tmpresponse = requests.get(tmpurl)
    data = tmpresponse.json()
    ## save the inheritance pattern data
    try:
        pattern_nos = data['inheritance-pattern-list']
        i=0
        while i < len(pattern_nos):
            inher_dict = pattern_nos[i]['inheritance-pattern']
            inher_dict['topic']=data['name']
            inher_dict['url'] = eachurl
            inher_list.append(inher_dict)
            i=i+1
    except:
        inher_fail.append({'topic':data['name'],'url':eachurl})
    
    ## save the synonym list
    try:
        synlist = data['synonym-list']
        syndf = pd.DataFrame(synlist)
        syndf['topic']=data['name']
        synonyms_df = pd.concat((synonyms_df,syndf),ignore_index=True)
    except:
        syn_fail.append({'topic':data['name'],'url':eachurl})
    
    ## save the xrefs
    try:
        xreflist = data['db-key-list']
        k=0
        while k < len(xreflist):
            tmpdict = xreflist[k]['db-key']
            tmpdict['topic'] = data['name']
            tmpdict['url'] = eachurl
            xref_list.append(tmpdict)
            k=k+1
    except:
        xref_fail.append({'topic':data['name'],'url':eachurl})
    u=u+1

inheritance_df = pd.DataFrame(inher_list)
inher_fail_df = pd.DataFrame(inher_fail)
syn_fail_df = pd.DataFrame(syn_fail)
xref_list_df = pd.DataFrame(xref_list)
xref_fail_df = pd.DataFrame(xref_fail)



#### Use xrefs pulled from the API to map the url to Wikidata Entities
## Drop topics that map to the same url (assuming they're synonyms)
xref_no_dups = xref_list_df.drop_duplicates()
print("original df size: ",len(xref_list_df),"de-duplicated url df size: ",len(xref_no_dups))

## Use Orphanet IDs to pull Wikidata Entities
## Generate list of unique Orphanet IDs
orphanet_ghr = xref_no_dups.loc[xref_no_dups['db']=='Orphanet']
no_orphanet_dups = orphanet_ghr.drop_duplicates('url')
print("Original Orphanet Xref list: ", len(orphanet_ghr), "Orphanet Xref list less dups: ",len(no_orphanet_dups))
orphanet_id_list = no_orphanet_dups['key'].tolist()

# Retrieve the QIDs for each Orphanet ID (The property for Orphanet IDs is P1550)
i=0
wdmap = []
wdmapfail = []
for i in tqdm(range(len(orphanet_id_list))):
    orph_id = orphanet_id_list[i]
    try:
        sparqlQuery = "SELECT * WHERE {?topic wdt:P1550 \""+orph_id+"\"}"
        result = wdi_core.WDItemEngine.execute_sparql_query(sparqlQuery)
        orpha_qid = result["results"]["bindings"][0]["topic"]["value"].replace("http://www.wikidata.org/entity/", "")
        wdmap.append({'Orphanet':orph_id,'WDID':orpha_qid})
    except:
        wdmapfail.append(orph_id)
    i=i+1

## Inspect the results for mapping or coverage issues
wdid_orpha_df = pd.DataFrame(wdmap)
print("resulting mapping table has: ",len(wdid_orpha_df)," rows.")



#### Add Mode of Inheritance data from GHR to Wikidata
## De-duplicate to remove anything with mapping issues
wd_orpha_no_dups = wdid_orpha_df.drop_duplicates('Orphanet').copy()
wd_orpha_no_dups.drop_duplicates('WDID')
print('de-duplicated table: ',len(wd_orpha_no_dups))

## Merge with Inheritance table
no_orphanet_dups.rename(columns={'key':'Orphanet'}, inplace=True)
inher_wd_db = inheritance_df.merge(wd_orpha_no_dups.merge(no_orphanet_dups,on='Orphanet',how='inner'), on=['url','topic'], how='inner')
print("resulting mapped table: ",len(inher_wd_db))

## Limit adding mode of inheritance statements to diseases with known modes of inheritance
inheritance_avail = inher_wd_db.loc[(inher_wd_db['code']!='n')&(inher_wd_db['code']!='u')]
print(len(inheritance_avail))

## Perform the entity look up and write the inheritance mode statement
i=0
for i in tqdm(range(len(inheritance_avail))):
    disease_qid = inheritance_avail.iloc[i]['WDID']
    inheritance_method = GHR_WD_codes[inheritance_avail.iloc[i]['code']]
    ghr_url = inheritance_avail.iloc[i]['url']
    reference = create_reference(ghr_url)
    statement = [wdi_core.WDItemID(value=inheritance_method, prop_nr="P1199", references=[copy.deepcopy(reference)])]
    item = wdi_core.WDItemEngine(wd_item_id=disease_qid, data=statement, append_value="P1199",
                           global_ref_mode='CUSTOM', ref_handler=update_retrieved_if_new_multiple_refs)
    item.write(login)
    i=i+1




#### Add GHR disease/conditions urls (once property has been created and approved)
## Load successfully mapped GHR disease urls
mapped_orpha_urls = wd_orpha_no_dups.merge(no_orphanet_dups,on='Orphanet',how='inner')

i=0
for i in tqdm(range(len(mapped_orpha_urls))):
    disease_qid = mapped_orpha_urls.iloc[i]['WDID']
    ghr_url = mapped_orpha_urls.iloc[i]['url']
    ghr_id = mapped_orpha_urls.iloc[0]['url'].replace("https://ghr.nlm.nih.gov/condition/","")
    reference = create_reference(ghr_url)
    url_prop = "P7464" 
    statement = [wdi_core.WDString(value=ghr_id, prop_nr=url_prop, references=[copy.deepcopy(reference)])]
    item = wdi_core.WDItemEngine(wd_item_id=disease_qid, data=statement, append_value=url_prop,
                               global_ref_mode='CUSTOM', ref_handler=update_retrieved_if_new_multiple_refs)
    item.write(login)
    i=i+1



