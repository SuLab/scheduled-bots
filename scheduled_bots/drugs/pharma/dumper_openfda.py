# checks https://open.fda.gov/downloads/
# to see when last updated and get the file download urls
# this page calls https://api.fda.gov/download.json to get the actual info on it
import subprocess

import requests
from dateutil.parser import parse


class Dumper:
    def __init__(self):
        d = requests.get("https://api.fda.gov/download.json").json()
        self.drugs = d['results']['drug']['label']

    def get_last_updated(self):
        return parse(self.drugs['export_date'])

    def get_download_links(self):
        return [x['file'] for x in self.drugs['partitions']]

    def download_files(self):
        for file in self.get_download_links():
            subprocess.check_call(["wget", "-N", file])