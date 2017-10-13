import os
import json
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from functools import partial
from time import strftime, gmtime
from wikidataintegrator import ref_handlers
from wikidataintegrator import wdi_core, wdi_login, wdi_helpers, wdi_property_store

update_retrieved_if_new = partial(ref_handlers.update_retrieved_if_new, days=180)

wdi_property_store.wd_properties['P4333'] = {
    'core_id': True
}
try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

PROPS = {
    'NCBI Taxonomy ID': 'P685',
    'GenBank Assembly accession': 'P4333',
    'stated in': 'P248',
    'retrieved': 'P813',
    'reference URL': 'P854'
}

ITEMS = {
    'GenBank': 'Q901755'
}


def load_data():
    # load in csv
    df = pd.read_csv("prokaryotes.txt", sep='\t', low_memory=False)
    # filter for complete genomes only
    df = df.query("Status == 'Complete Genome'")
    # create a dict where the key is the taxID, value is the list of accessions for that taxID
    d = df.groupby("TaxID").agg({'Assembly Accession': lambda x: list(x)}).to_dict()['Assembly Accession']
    # filter out the ones where there is more than one accession
    d = {k: v[0] for k, v in d.items() if len(v) == 1}
    return d


def download_data():
    os.system(u'wget -nc ftp://ftp.ncbi.nlm.nih.gov/genomes/GENOME_REPORTS/prokaryotes.txt')


def create_reference(genbank_id):
    stated_in = wdi_core.WDItemID(ITEMS['GenBank'], PROPS['stated in'], is_reference=True)
    retrieved = wdi_core.WDTime(strftime("+%Y-%m-%dT00:00:00Z", gmtime()), PROPS['retrieved'], is_reference=True)
    url = "https://www.ncbi.nlm.nih.gov/genome/?term={}".format(genbank_id)
    ref_url = wdi_core.WDUrl(url, PROPS['reference URL'], is_reference=True)
    return [stated_in, retrieved, ref_url]


def run_one(taxid, genbank_id):
    # get the QID
    taxid = str(taxid)
    if taxid not in tax_qid_map:
        msg = wdi_helpers.format_msg(genbank_id, PROPS['GenBank Assembly accession'], "",
                                     "organism with taxid {} not found or skipped".format(taxid))
        wdi_core.WDItemEngine.log("WARNING", msg)
        return None
    qid = tax_qid_map[taxid]
    reference = create_reference(genbank_id)
    genbank_statement = wdi_core.WDExternalID(genbank_id, PROPS['GenBank Assembly accession'], references=[reference])

    # create the item object, specifying the qid
    item = wdi_core.WDItemEngine(data=[genbank_statement], wd_item_id=qid, fast_run=True,
                                 fast_run_base_filter={PROPS['GenBank Assembly accession']: ''},
                                 global_ref_mode='CUSTOM', fast_run_use_refs=True,
                                 ref_handler=update_retrieved_if_new)

    wdi_helpers.try_write(item, record_id=genbank_id, record_prop=PROPS['GenBank Assembly accession'],
                          login=login, edit_summary="update GenBank Assembly accession")


if __name__ == "__main__":
    login = wdi_login.WDLogin(WDUSER, WDPASS)
    wdi_core.WDItemEngine.setup_logging(header=json.dumps(
        {'name': 'genbank assembly', 'timestamp': str(datetime.now()), 'run_id': str(datetime.now())}))
    # instead of using wdi and search_only to retrieve the item, we'll do it manually, all at once
    tax_qid_map = wdi_helpers.id_mapper(PROPS['NCBI Taxonomy ID'], return_as_set=True)
    # filter out those where the same taxid is used across more than one item
    tax_qid_map = {k: list(v)[0] for k, v in tax_qid_map.items() if len(v) == 1}

    download_data()
    d = load_data()
    for taxid, genbank_id in tqdm(d.items()):
        run_one(taxid, genbank_id)
