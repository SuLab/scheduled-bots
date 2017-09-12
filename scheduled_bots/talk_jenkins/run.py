"""
Hit jenkins API to get the status of all jobs. 
Creates a table at: https://www.wikidata.org/w/index.php?title=User:ProteinBoxBot/Bot_Status

api example: http://jenkins.sulab.org/job/Gene_Ontology/7/api/python?pretty=true

"""
import os
import jenkins
import pandas as pd
import requests
from datetime import datetime

from scheduled_bots.utils import login_to_wikidata, pd_to_table

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
HOST = "http://jenkins.sulab.org"

server = jenkins.Jenkins(HOST, username='admin', password=JENKINS_PASS)
data = []
jobs = server.get_jobs(view_name="Running")
for job in jobs:
    job_name = job['name']
    job_info = server.get_job_info(job_name)
    job_descr = job_info['description']

    if job_info['lastBuild'] is None:
        # this job has never been run
        continue

    last_build = job_info['lastBuild']['number']
    build_info = server.get_build_info(job_name, last_build)

    if build_info['building']:
        # this job is currently running
        # use the 'lastCompletedBuild'
        last_build = job_info['lastCompletedBuild']['number']
        build_info = server.get_build_info(job_name, last_build)

    """
    # add optional metadata
    CODE_URL: url to code
    WDUSER: name of the wikidata account running this code
    """
    wduser = None
    code_url = None
    if "actions" in build_info:
        actions = [x for x in build_info["actions"] if '_class' in x]
        classes = [x["_class"] for x in actions]
        if classes.count("hudson.model.ParametersAction") == 1:
            params = [x['parameters'] for x in actions if x["_class"] == "hudson.model.ParametersAction"][0]
            for param in params:
                if param['name'] == "WDUSER":
                    wduser = param['value']
                elif param['name'] == "CODE_URL":
                    code_url = param['value']

    last_build_success = build_info['result']
    timestamp_int = build_info['timestamp']
    timestamp = datetime.fromtimestamp(int(timestamp_int) / 1000).strftime('%Y-%m-%d %H:%M:%S')
    log_paths = []
    log_reports = []
    if len(build_info['artifacts']):
        for artifact in build_info['artifacts']:
            log_path = HOST + "/job/{job_name}/{job_num}/artifact/".format(job_name=job_name, job_num=last_build) + \
                       artifact['relativePath']
            if log_path.endswith(".html"):
                log_reports.append(log_path)
            else:
                log_paths.append(log_path)

    data.append({'Job Name': job_name, 'Description': job_descr, 'Last Run Status': last_build_success,
                 'Log Path': log_paths, 'Last Run': timestamp, 'Code URL': code_url, 'Bot Account': wduser,
                 'Job Path': HOST + "/job/{job_name}/{job_num}/".format(job_name=job_name, job_num=last_build),
                 'Log Report': log_reports})


def make_links(lst):
    # if there are more than 10 links, don't show all of the names
    if len(lst) < 10:
        s = "; ".join(["[{} {}]".format(x.replace(" ", "%20"), x.split("/")[-1]) for x in lst])
    else:
        s = "; ".join(["[{} {}]".format(x.replace(" ", "%20"), n) for n, x in enumerate(lst)])
    return s


df = pd.DataFrame(data)
df['Job Name'] = df.apply(lambda x: "[{} {}]".format(x['Job Path'].replace(" ", "%20"), x['Job Name']), axis=1)
df['Log Path'] = df['Log Path'].apply(lambda lst: make_links(lst) if lst else '')
df['Log Report'] = df['Log Report'].apply(lambda lst: make_links(lst) if lst else '')
df['Code URL'] = df['Code URL'].apply(lambda x: "[{} Link]".format(x) if x else '')
df['Bot Account'] = df['Bot Account'].apply(
    lambda x: "[https://www.wikidata.org/wiki/User:{} {}]".format(x, x) if x else '')
df = df[['Job Name', 'Description', 'Last Run Status', 'Last Run', 'Log Report', 'Log Path', 'Code URL', 'Bot Account']]
print(df)
mwtable = pd_to_table(df)
print(mwtable)
edit_token, edit_cookie = login_to_wikidata(WDUSER, WDPASS)
data = {'action': 'edit', 'title': 'User:ProteinBoxBot/Bot_Status',
        'text': mwtable, 'format': 'json', 'token': edit_token}
r = requests.post("https://www.wikidata.org/w/api.php", data=data, cookies=edit_cookie)
r.raise_for_status()
