import chemspipy
import time

from scheduled_bots.local import CHEMSPIDER_TOKEN
from wikidataintegrator import wdi_core


class ChemSpiderMolecule(object):
    def __init__(self, csid=None, mol=None, inchi_key=None, verbose=False):
        if csid:
            cs = chemspipy.ChemSpider(CHEMSPIDER_TOKEN)
            self.compound = cs.get_compound(csid)
        elif mol:
            self.compound = mol
        elif inchi_key:
            self.compound = ChemSpiderMolecule.get_by_inchikey(inchi_key)

        if verbose:
            x = self.compound
            print(x.common_name)
            print(x.stdinchikey)
            print(x.stdinchi)
            print(x.csid)

    @property
    def stdinchikey(self):
        return self.compound.stdinchikey

    @property
    def stdinchi(self):
        return self.compound.stdinchi

    @property
    def common_name(self):
        try:
            return self.compound.common_name
        except KeyError:
            return None

    @property
    def csid(self):
        return str(self.compound.csid)

    @property
    def monoisotopic_mass(self):
        return self.compound.monoisotopic_mass

    @property
    def label(self):
        item_label = self.common_name if self.common_name else self.csid
        return item_label

    @staticmethod
    def search(search_string):
        molecules = []

        cs = chemspipy.ChemSpider(CHEMSPIDER_TOKEN)

        for x in cs.search(search_string):
            molecules.append(ChemSpiderMolecule(mol=x))
        return molecules

    @staticmethod
    def search_one(search_string):
        mols = ChemSpiderMolecule.search(search_string)
        if len(mols) == 1:
            return ChemSpiderMolecule(mol=mols[0])
        else:
            return None

    @staticmethod
    def get_by_inchikey(inchi):
        mols = ChemSpiderMolecule.search(inchi)
        mols = [m for m in mols if m.stdinchikey == inchi]
        if len(mols) == 1:
            return ChemSpiderMolecule(mol=mols[0])
        else:
            raise ValueError("no chemspider matches: {}".format(inchi))

    def to_wikidata(self):

        ref = [[
            wdi_core.WDItemID(value='Q2311683', prop_nr='P248', is_reference=True),  # stated in
            wdi_core.WDExternalID(value=self.csid, prop_nr='P661', is_reference=True),  # source element
            wdi_core.WDTime(time=time.strftime('+%Y-%m-%dT00:00:00Z'), prop_nr='P813', is_reference=True)  # retrieved
        ]]
        print('Main label is', self.label)

        elements = {
            'P661': self.csid,
            'P235': self.stdinchikey,
            'P234': self.stdinchi[6:],
        }

        # do not try to add InChI longer than 400 chars
        if len(self.stdinchi[6:]) > 400:
            del elements['P234']

        data = []
        if float(self.monoisotopic_mass) != 0:
            data = [
                wdi_core.WDQuantity(value=self.monoisotopic_mass, prop_nr='P2067', upper_bound=self.monoisotopic_mass,
                                    lower_bound=self.monoisotopic_mass, unit='http://www.wikidata.org/entity/Q483261',
                                    references=ref)
            ]

        for k, v in elements.items():
            if not v:
                continue

            print('{}:'.format(k), v)
            if isinstance(v, list) or isinstance(v, set):
                for x in v:
                    data.append(wdi_core.WDString(prop_nr=k, value=x, references=ref))
            else:
                data.append(wdi_core.WDString(prop_nr=k, value=v, references=ref))

        return data
