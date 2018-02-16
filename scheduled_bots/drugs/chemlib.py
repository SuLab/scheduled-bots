import os
import traceback

from tqdm import tqdm

from scheduled_bots.drugs.chemspider import ChemSpiderMolecule
from scheduled_bots.drugs.unii import UNIIMolecule
from scheduled_bots.drugs.pubchem import PubChemMolecule
from wikidataintegrator import wdi_core, wdi_login
from wikidataintegrator import wdi_property_store

try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS)

MOL_TYPES = [UNIIMolecule, ChemSpiderMolecule, PubChemMolecule]
# MOL_TYPES = [UNIIMolecule]


for x in wdi_property_store.wd_properties.values():
    x['core_id'] = False
wdi_property_store.wd_properties['P652']['core_id'] = True
wdi_property_store.wd_properties['P231']['core_id'] = True

# inchi_key = "MIXMJCQRHVAJIO-TZHJZOAOSA-N"

def create_item(inchi_key):
    mol_instances = []
    wd_data = []
    curr_props = set()

    for mol_type in MOL_TYPES:
        try:
            mol = mol_type(inchi_key=inchi_key)
            mol_instances.append(mol)
            wd_data.extend([x for x in mol.to_wikidata() if x.get_prop_nr() not in curr_props])
            curr_props.update(set([x.get_prop_nr() for x in wd_data]))
        except Exception as e:
            traceback.print_exc()
            print(e)

    print([(x.get_prop_nr(), x.get_value()) for x in wd_data])

    # wd_data.append(wdi_core.WDItemID(value='Q11173', prop_nr='P31'))
    # make sure that core ids are in the data, skip if not
    core_ids = {'P662', 'P661', 'P683', 'P652', 'P235', 'P234', 'P715'}
    core_prop_matches = [True for x in wd_data if x.get_prop_nr() in core_ids]
    # assert len(core_prop_matches) >= 3
    wd_item = wdi_core.WDItemEngine(item_name='drug', domain='drugs', data=wd_data,
                                    append_value=['P31', 'P235', 'P233', 'P2017'])
    if wd_item.create_new_item:
        return None

    # set label using the first class in mol_types
    # wd_item.set_label(mol_instances[0].label)
    # only set 'chemical compound' as description if nothing else exists
    if not wd_item.get_description():
        wd_item.set_description('chemical compound')
    # make aliases
    aliases = set(m.label for m in mol_instances) - {mol_instances[0].label} - {None, ''}
    print("aliases: {}".format(aliases))
    wd_item.set_aliases(aliases, append=True)

    qid = wd_item.write(login)
    return qid