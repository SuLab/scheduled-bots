"""
Note: this is a really abbreviated version of sebastian's full pubchem bot
that simply gets the pubchem ID from an inchikey

Adapted from: https://github.com/sebotic/cdk_pywrapper/blob/master/cdk_pywrapper/chemlib.py
"""
import json
import time

import requests

import wikidataintegrator.wdi_core as wdi_core


class PubChemMolecule(object):
    headers = {
        'accept': 'application/json',
        'content-type': 'application/json',
        'charset': 'utf-8'
    }

    base_url = 'http://pubchem.ncbi.nlm.nih.gov/rest/rdf/{}'

    def __init__(self, cid=None, inchi_key=None):
        if cid:
            self.cid = cid
        if inchi_key:
            self.stdinchikey = inchi_key

        if cid:
            pass
        elif inchi_key:
            cids = self._retrieve_pubchem_cids(self.stdinchikey)
            if len(cids) == 0:
                raise ValueError('InChI key not found in PubChem!')
            if len(cids) == 1:
                self.cid = cids[0]
            else:
                raise ValueError('More than one result: {}'.format(cids))

    @staticmethod
    def _retrieve_basic_compound_info(cid):
        cmpnd_url = 'https://pubchem.ncbi.nlm.nih.gov/rest/rdf/compound/{}.json'.format(cid)
        print(cmpnd_url)

        # r = PubChemMolecule.s.get(cmpnd_url, headers=PubChemMolecule.headers).json()
        r = requests.get(cmpnd_url, headers=PubChemMolecule.headers).json()

        return r

    @staticmethod
    def _retrieve_pubchem_cids(ikey):
        url = 'http://pubchem.ncbi.nlm.nih.gov/rest/rdf/inchikey/{}.json'.format(ikey)

        try:
            # r = PubChemMolecule.s.get(url, headers=PubChemMolecule.headers).json()
            r = requests.get(url, headers=PubChemMolecule.headers).json()
        except json.JSONDecodeError as e:
            # print(e.__str__())
            print('PubChem does not have this InChI key', ikey)
            return []

        cids = list()
        if 'http://semanticscience.org/resource/is-attribute-of' in r['inchikey/{}'.format(ikey)]:
            for x in r['inchikey/{}'.format(ikey)]['http://semanticscience.org/resource/is-attribute-of']:
                cids.append(x['value'].split('/')[-1])

        return cids

    @property
    def label(self):
        return None

    def to_wikidata(self):

        refs = [[
            wdi_core.WDItemID(value='Q278487', prop_nr='P248', is_reference=True),  # stated in
            wdi_core.WDExternalID(value=self.cid, prop_nr='P662', is_reference=True),  # source element
            wdi_core.WDTime(time=time.strftime('+%Y-%m-%dT00:00:00Z'), prop_nr='P813', is_reference=True)  # retrieved
        ]]

        elements = {
            'P662': self.cid[3:]
        }

        data = []

        for k, v in elements.items():
            if not v:
                continue

            print('{}:'.format(k), v)
            if isinstance(v, list) or isinstance(v, set):
                for x in v:
                    data.append(wdi_core.WDString(prop_nr=k, value=x, references=refs))
            else:
                data.append(wdi_core.WDString(prop_nr=k, value=v, references=refs))

        return data
