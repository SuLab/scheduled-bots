"""
Bot code for creating chemical items in wikidata from UNII

Adapted from: https://github.com/sebotic/cdk_pywrapper/blob/master/cdk_pywrapper/chemlib.py
"""
import os
import re
import subprocess
import time
import zipfile
import pandas as pd
import wikidataintegrator.wdi_core as wdi_core

data_folder = "unii_data"


class UNIIMolecule(object):
    unii_path = os.path.join(data_folder, 'unii_data.txt')
    if not os.path.exists(unii_path):
        load_unii()
    unii_df = pd.read_csv(unii_path, dtype=str, sep='\t', low_memory=False)

    def __init__(self, unii=None, inchi_key=None, verbose=False):

        if unii:
            ind = UNIIMolecule.unii_df['UNII'].values == unii
        else:
            ind = UNIIMolecule.unii_df['INCHIKEY'].values == inchi_key

        self.data = UNIIMolecule.unii_df.loc[ind, :]

        if len(self.data.index) != 1:
            raise ValueError('Provided ID did not return a unique UNII')

        self.data_index = self.data.index[0]

        if verbose:
            x = self.data
            print(x.common_name)
            print(x.stdinchikey)
            print(x.stdinchi)
            print(x.csid)

    @property
    def stdinchikey(self):
        ikey = self.data.loc[self.data_index, 'INCHIKEY']
        if pd.isnull(ikey) and pd.isnull(self.smiles):
            return None

        return ikey

    @property
    def stdinchi(self):
        if pd.isnull(self.smiles):
            return None

    @property
    def preferred_name(self):
        name = self.data.loc[self.data_index, 'PT']
        return UNIIMolecule.label_converter(name) if pd.notnull(name) else None

    @property
    def smiles(self):
        smiles = self.data.loc[self.data_index, 'SMILES']
        return smiles if pd.notnull(smiles) else None

    @property
    def molecule_type(self):
        molecule_type = self.data.loc[self.data_index, 'UNII_TYPE']
        return molecule_type if pd.notnull(molecule_type) else None

    @property
    def unii(self):
        return self.data.loc[self.data_index, 'UNII']

    @property
    def cas(self):
        cas = self.data.loc[self.data_index, 'RN']
        return cas if pd.notnull(cas) else None

    @property
    def einecs(self):
        einecs = self.data.loc[self.data_index, 'EC']
        return einecs if pd.notnull(einecs) else None

    @property
    def rxnorm(self):
        rxnorm = self.data.loc[self.data_index, 'RXCUI']
        return rxnorm if pd.notnull(rxnorm) else None

    @property
    def nci(self):
        nci = self.data.loc[self.data_index, 'NCIT']
        return nci if pd.notnull(nci) else None

    @property
    def umls(self):
        umls_cui = self.data.loc[self.data_index, 'UMLS_CUI']
        return umls_cui if pd.notnull(umls_cui) else None

    @property
    def pubchem(self):
        pubchem = self.data.loc[self.data_index, 'PUBCHEM']
        return pubchem if pd.notnull(pubchem) else None

    @property
    def label(self):
        item_label = self.preferred_name if self.preferred_name else self.unii
        return item_label

    def to_wikidata(self):

        refs = [[
            wdi_core.WDItemID(value='Q6593799', prop_nr='P248', is_reference=True),  # stated in
            wdi_core.WDExternalID(value=self.unii, prop_nr='P652', is_reference=True),  # source element
            wdi_core.WDTime(time=time.strftime('+%Y-%m-%dT00:00:00Z'), prop_nr='P813', is_reference=True)  # retrieved
        ]]
        print('UNII Main label is', self.label)

        elements = {
            'P652': self.unii,
            'P2017': self.smiles,
            'P235': self.stdinchikey,
            'P231': self.cas,
            'P232': self.einecs,
            'P1748': self.nci,
            'P3345': self.rxnorm
        }

        if self.smiles and len(self.smiles) > 400:
            del elements['P2017']

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

    @staticmethod
    def label_converter(label):
        label = label.lower()

        greek_codes = {
            '.alpha.': '\u03B1',
            '.beta.': '\u03B2',
            '.gamma.': '\u03B3',
            '.delta.': '\u03B4',
            '.epsilon.': '\u03B5',
            '.zeta.': '\u03B6 ',
            '.eta.': '\u03B7',
            '.theta.': '\u03B8',
            '.iota.': '\u03B9',
            '.kappa.': '\u03BA',
            '.lambda.': '\u03BB',
            '.mu.': '\u03BC',
            '.nu.': '\u03BD',
            '.xi.': '\u03BE',
            '.omicron.': '\u03BF',
            '.pi.': '\u03C0',
            '.rho.': '\u03C1',
            '.sigma.': '\u03C3',
            '.tau.': '\u03C4',
            '.upsilon.': '\u03C5',
            '.phi.': '\u03C6',
            '.chi.': '\u03C7',
            '.psi.': '\u03C8',
            '.omega.': '\u03C9',

        }

        for greek_letter, unicode in greek_codes.items():
            if greek_letter in label:
                label = label.replace(greek_letter, unicode)

        match = re.compile('(^|[^a-z])([ezdlnhros]{1}|dl{1})[^a-z]{1}')

        while True:
            if re.search(match, label):
                replacement = label[re.search(match, label).start(): re.search(match, label).end()].upper()
                label = re.sub(match, repl=replacement, string=label, count=1)
            else:
                break

        splits = label.split(', ')
        splits.reverse()
        return ''.join(splits)


def load_unii():
    url = 'http://fdasis.nlm.nih.gov/srs/download/srs/UNII_Data.zip'
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)

    subprocess.check_call(["wget", "-N", "-P", data_folder, url])

    with zipfile.ZipFile(os.path.join(data_folder, 'UNII_Data.zip'), 'r') as zf:
        zf.extractall(data_folder)

    for file in os.listdir(data_folder):
        if 'Records' in file:
            full_file_name = os.path.join(data_folder, file)
            os.rename(full_file_name, os.path.join(data_folder, 'unii_data.txt'))
