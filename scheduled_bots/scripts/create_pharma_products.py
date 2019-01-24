"""
One off script to create pharmaceutical products

Requires CSV containing columns:
drug_qid,brand_rxnorm,emea,url,normalized_name

Definitions:
drug_qid: QID of the active ingredient
brand_rxnorm: rxnorm CUI of the product/brand (optional)
emea: EMEA ID of the product/brand
url: URL for EMEA to use as the ref
normalized_name: name of the product/brand to use on the wikidata item page, this also gets removed from the aliases on the active ingredient

"""
import pandas as pd
from time import gmtime, strftime
from tqdm import tqdm
from wikidataintegrator import wdi_core, wdi_login, wdi_helpers, wdi_property_store
from scheduled_bots.local import WDPASS, WDUSER
login = wdi_login.WDLogin(WDUSER, WDPASS)

wdi_property_store.wd_properties['P3637'] = {
        'datatype': 'string',
        'name': 'EMEA ID',
        'domain': ['drugs'],
        'core_id': 'True'
    }

def create_ref_statement(emea_id, url):
    ref_url = wdi_core.WDUrl(url, prop_nr='P854', is_reference=True)
    ref_emea = wdi_core.WDExternalID(emea_id, 'P3637', is_reference=True)
    ref_retrieved = wdi_core.WDTime(strftime("+%Y-%m-%dT00:00:00Z", gmtime()), prop_nr='P813', is_reference=True)
    reference = [ref_emea, ref_url, ref_retrieved]
    return reference

title_case = lambda name: ' '.join([x[0].upper() + x[1:] for x in name.split(" ")])


def do_pharm_prod(drug_qid, brand_rxnorm, emea, url, brand_name):
    # write info on the pharmaceutical product page
    ref = create_ref_statement(emea, url)
    # has active substance
    s = [wdi_core.WDItemID(drug_qid, 'P3781', references=[ref])]
    # instance of
    s.append(wdi_core.WDItemID('Q28885102', 'P31', references=[ref]))  # pharmaceutical product
    s.append(wdi_core.WDItemID('Q169336', 'P31', references=[ref]))  # chemical mixture
    # emea
    s.append(wdi_core.WDExternalID(emea, 'P3637', references=[ref]))

    if not pd.isnull(brand_rxnorm):
        s.append(wdi_core.WDExternalID(str(int(brand_rxnorm)), "P3345"))
    item = wdi_core.WDItemEngine(data=s, append_value=['P3781'])
    item.set_label(brand_name)
    if item.get_description() == '':
        item.set_description("pharmaceutical product")
    wdi_helpers.try_write(item, emea, 'P3637', login, edit_summary="add 'active ingredient'")

    return item.wd_item_id


def do_compound(brand_qid, drug_qid, brand_name):
    # on the drug, add "active ingredient in"
    # remove brand name from aliases if there
    ref = create_ref_statement(emea, url)
    s = [wdi_core.WDItemID(brand_qid, 'P3780', references=[ref])]
    item = wdi_core.WDItemEngine(wd_item_id=drug_qid, data=s, append_value=['P3780'])
    aliases = item.get_aliases()
    aliases = [x for x in aliases if brand_name.lower() != x.lower()]
    item.set_aliases(aliases, append=False)
    wdi_helpers.try_write(item, '', '', login, edit_summary="add 'active ingredient in'")

df = pd.read_csv("ema_do_hp_rxnorm_snomed.csv", index_col=0)
df = df.dropna(subset=['drug_qid', 'url'])

for _,row in tqdm(df.iterrows(), total=len(df)):
    drug_qid = row.drug_qid
    brand_rxnorm = row.brand_rxnorm
    emea = row.emea
    url = row.url
    brand_name = title_case(row.normalized_name)
    brand_qid = do_pharm_prod(drug_qid, brand_rxnorm, emea, url, brand_name)
    do_compound(brand_qid, drug_qid, brand_name)