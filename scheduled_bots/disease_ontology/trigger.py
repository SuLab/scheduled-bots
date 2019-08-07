import os
from datetime import datetime
import requests
import json

URL = 'http://purl.obolibrary.org/obo/doid/releases/{}/doid.json'


def get_latest_release():
    url = "https://api.github.com/repos/DiseaseOntology/HumanDiseaseOntology/contents/src/ontology/releases"
    res = requests.get(url).json()
    res = [r for r in res if r['type'] == 'dir']
    dates = []
    for r in res:
        try:
            dates.append(datetime.strptime(r['name'], '%Y-%m-%d'))
        except Exception as e:
            print(e)
    latest_release = max(dates).strftime("%Y-%m-%d")
    return latest_release


def get_release_obographs(d):
    graphs = {g['id']: g for g in d['graphs']}
    graph = graphs['http://purl.obolibrary.org/obo/doid.owl']
    version_purl = graph['meta']['version']
    version = version_purl.split("/")[-2:][0]
    return version


def main():
    # expects a file in the current folder called 'doid.json'
    # if not found, will always trigger
    if os.path.exists("TRIGGER"):
        os.remove("TRIGGER")
        assert not os.path.exists("TRIGGER")
    if not os.path.exists("doid.json"):
        print("doid.json not found. triggering job")
        current_release = ""
    else:
        with open("doid.json") as f:
            d = json.load(f)
        current_release = get_release_obographs(d) # download
    print("current release: {}".format(current_release))
    latest_release = get_latest_release()
    print("latest release: {}".format(latest_release))

    if current_release != latest_release:
        print("downloading new release & triggering job")
        os.system("wget -N {}".format(URL.format(latest_release))) # here the latest release of the disease ontology is deleted
        os.system("touch TRIGGER")
    else:
        print("not running job")


if __name__ == "__main__":
    main()
