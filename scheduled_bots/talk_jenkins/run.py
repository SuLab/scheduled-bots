"""
Hit jenkins API to get the status of all jobs. 
Creates a table at: https://www.wikidata.org/w/index.php?title=User:ProteinBoxBot/Bot_Status

"""
import os
import jenkins
import pandas as pd
import requests
from datetime import datetime


try:
    from scheduled_bots.local import JENKINS_PASS, WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ and "JENKINS_PASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
        JENKINS_PASS = os.environ['JENKINS_PASS']
    else:
        raise ValueError("WDUSER and WDPASS and JENKINS_PASS must be specified in local.py or as environment variables")



# jenkins url
HOST = "http://52.15.200.208:8080"

def login(USER, PASS):
	baseurl = 'https://www.wikidata.org/w/'
	# Login request
	payload = {'action': 'query', 'format': 'json', 'utf8': '', 'meta': 'tokens', 'type': 'login'}
	r1 = requests.post(baseurl + 'api.php', data=payload)
	# login confirm
	login_token = r1.json()['query']['tokens']['logintoken']
	payload = {'action': 'login', 'format': 'json', 'utf8': '', 'lgname': USER, 'lgpassword': PASS, 'lgtoken': login_token}
	r2 = requests.post(baseurl + 'api.php', data=payload, cookies=r1.cookies)
	# get edit token2
	params3 = '?format=json&action=query&meta=tokens&continue='
	r3 = requests.get(baseurl + 'api.php' + params3, cookies=r2.cookies)
	edit_token = r3.json()['query']['tokens']['csrftoken']
	edit_cookie = r2.cookies.copy()
	edit_cookie.update(r3.cookies)
	return edit_token, edit_cookie


def pd_to_table(df):
    # quick n dirty pandas DataFrame to mediawikitable converter
    """{| border="1" class="dataframe"
        |- style="text-align: right;"
        !
        ! Article Name!! wikidata ID!! InterPro Items!! InterPro WDIDs!! About Gene!! Done
        |-
        ! 0
        |[[:en:Tetratricopeptide|Tetratricopeptide]]||[[Q7706768]]||[www.ebi.ac.uk/interpro/entry/IPR001440 IPR001440]||[[Q24779822]]||False||False
        |-
        ! 1
        |[[:en:UDP-N-acetylglucosamine 2-epimerase|UDP-N-acetylglucosamine 2-epimerase]]||[[Q13411653]]||[www.ebi.ac.uk/interpro/entry/IPR003331 IPR003331]||[[Q24721922]]||False||False
        |}
    """
    out = "{| border='1' class='wikitable sortable table-yes table-no' style='text-align: left;'\n!\n"
    out += '!'.join(['! {}'.format(x) for x in list(df.columns)])
    for record in df.to_records():
        record = list(record)
        record = list(map(lambda x:x.replace("\r\n", "<br>") if isinstance(x, str) else x, record))
        out += "\n|-\n"
        out += "! " + str(record[0]) + '\n'
        out += '|'.join(['|{}'.format(x) for x in record[1:]])
    out += "\n|}"
    return out


server = jenkins.Jenkins(HOST, username='admin', password=JENKINS_PASS)
data = []
jobs = server.get_jobs(view_name="Running")
for job in jobs:
	job_name = job['name']
	job_info = server.get_job_info(job_name)
	job_descr = job_info['description']
	last_build_success = "Success" if job_info['color'] == 'blue' else "Failed"
	last_build = job_info['lastBuild']['number']
	build_info = server.get_build_info(job_name, last_build)
	timestamp_int = build_info['timestamp']
	timestamp = datetime.fromtimestamp(int(timestamp_int)/1000).strftime('%Y-%m-%d %H:%M:%S')
	log_path = (HOST + "/job/{job_name}/{job_num}/artifact/".format(job_name=job_name, job_num=last_build) 
		+ build_info['artifacts'][0]['relativePath']) if len(build_info['artifacts']) else None

	data.append({'Job Name': job_name, 'Description': job_descr, 'Last Run Status': last_build_success,
	 'Log Path': log_path, 'Last Run': timestamp})

df = pd.DataFrame(data)
df = df[['Job Name', 'Description', 'Last Run Status', 'Last Run', 'Log Path']]
df['Log Path'] = df['Log Path'].apply(lambda x: "[{} Log]".format(x) if x else '')
print(df)
mwtable = pd_to_table(df)

edit_token, edit_cookie = login(WDUSER, WDPASS)
data = {'action': 'edit', 'title': 'User:ProteinBoxBot/Bot_Status', 
		'text': mwtable, 'format': 'json', 'token': edit_token}
r = requests.post("https://www.wikidata.org/w/api.php", data=data, cookies=edit_cookie)
