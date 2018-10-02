import argparse
import json
import os
from collections import defaultdict
from datetime import datetime
from time import gmtime, strftime

import pandas as pd
from tqdm import tqdm

from scheduled_bots import PROPS, ITEMS, get_default_core_props
from wikidataintegrator import wdi_core, wdi_helpers, wdi_login
from wikidataintegrator.ref_handlers import update_retrieved_if_new_multiple_refs
from wikidataintegrator.wdi_helpers import try_write

__metadata__ = {
    'name': 'FAERSIndicationBot',
    'maintainer': 'GSS',
    'tags': ['drugs', 'indications'],
    'properties': [PROPS['drug used for treatment'], PROPS['medical condition treated']]
}
try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

"""
demo to check:
Insulin Glargine (Q417317)
metformin (Q19484)
hyperglycemia (Q271993)


"""


class FAERSBot:
    def __init__(self, drug_indications, login, write=True, run_one=False):
        self.drug_indications = drug_indications
        self.login = login
        self.write = write
        self.run_one = run_one

        self.core_props = set()
        self.append_props = [PROPS['drug used for treatment'], PROPS['medical condition treated']]
        self.refs = None
        self.create_reference()

        # make the reverse mapping as well
        indication_drugs = defaultdict(set)
        for drug, indications in drug_indications.items():
            for ind in indications:
                indication_drugs[ind].add(drug)
        self.indication_drugs = dict(indication_drugs)

        self.item_engine = self.make_item_engine()

    def make_item_engine(self):
        append_props = self.append_props
        core_props = self.core_props

        class SubCls(wdi_core.WDItemEngine):
            def __init__(self, *args, **kwargs):
                kwargs['domain'] = "fake news"
                kwargs['fast_run'] = False
                kwargs['item_name'] = "foo"
                kwargs['ref_handler'] = update_retrieved_if_new_multiple_refs
                kwargs['core_props'] = core_props
                kwargs['append_value'] = append_props
                super(SubCls, self).__init__(*args, **kwargs)

        return SubCls

    def create_reference(self):
        """
        Reference is:
        retrieved: date
        stated in: links to pmid items
        no reference URL
        """
        # Drug Indications Extracted from FAERS (Q56863002)
        ref = [wdi_core.WDItemID("Q56863002", PROPS['stated in'], is_reference=True)]
        t = strftime("+%Y-%m-%dT00:00:00Z", gmtime())
        ref.append(wdi_core.WDTime(t, prop_nr=PROPS['retrieved'], is_reference=True))
        self.refs = [ref]

    def run_one_drug(self, drug_qid, indications_qid):
        ss = []
        for indication_qid in indications_qid:
            s = wdi_core.WDItemID(indication_qid, PROPS['medical condition treated'], references=self.refs)
            ss.append(s)

        item = self.item_engine(wd_item_id=drug_qid, data=ss)
        assert not item.create_new_item

        try_write(item, record_id=drug_qid, record_prop=PROPS['medical condition treated'],
                  edit_summary="Add medical condition treated from faers", login=self.login, write=self.write)

    def run_one_indication(self, indication_qid, drugs_qid):
        ss = []
        for drug_qid in drugs_qid:
            s = wdi_core.WDItemID(drug_qid, PROPS['drug used for treatment'], references=self.refs)
            ss.append(s)

        item = self.item_engine(wd_item_id=indication_qid, data=ss)
        assert not item.create_new_item

        try_write(item, record_id=indication_qid, record_prop=PROPS['drug used for treatment'],
                  edit_summary="Add drug used for treatment from faers", login=self.login, write=self.write)

    def run(self):
        if self.run_one:
            if self.run_one in self.drug_indications:
                self.run_one_drug(self.run_one, self.drug_indications[self.run_one])
            elif self.run_one in self.indication_drugs:
                self.run_one_indication(self.run_one, self.indication_drugs[self.run_one])
            else:
                raise ValueError("{} not found".format(self.run_one))
            return None

        for drug_qid, indications_qid in tqdm(self.drug_indications.items()):
            self.run_one_drug(drug_qid, indications_qid)
        for indication_qid, drugs_qid in tqdm(self.indication_drugs.items()):
            self.run_one_indication(indication_qid, drugs_qid)


def load_data():
    df = pd.read_csv("https://zenodo.org/record/1436000/files/faers_indications.csv?download=1", index_col=0)
    df.indications_mondo = df.indications_mondo.str.split("|")
    df.dropna(subset=['indications_mondo', 'drug_rxcui'], inplace=True)

    return df


def normalize_to_qids(df: pd.DataFrame):
    rxnorm_qid = wdi_helpers.id_mapper(PROPS['RxNorm CUI'])
    mondo_qid = wdi_helpers.id_mapper(PROPS['Mondo ID'])

    df['drug_qid'] = df.drug_rxcui.astype(str).map(rxnorm_qid.get)
    df.dropna(subset=['drug_qid'], inplace=True)
    assert len(set(df.drug_qid)) == len(df)

    df['indications_qid'] = df.indications_mondo.apply(
        lambda x: set(mondo_qid[str(y)] for y in x if str(y) in mondo_qid))
    df.indications_qid = df.indications_qid.map(lambda x: x if x else None)
    df.dropna(subset=['indications_qid'], inplace=True)

    drug_indications = dict(zip(df.drug_qid, df.indications_qid))

    return drug_indications


def main(write=True, run_one=None):
    df = load_data()
    login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS)

    drug_indications = normalize_to_qids(df)

    bot = FAERSBot(drug_indications, login, write=write, run_one=run_one)
    bot.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='run faers indications bot')
    parser.add_argument('--dummy', help='do not actually do write', action='store_true')
    parser.add_argument('--run-one', help='run one drug or indication, by qid')
    args = parser.parse_args()
    log_dir = "./logs"
    run_id = datetime.now().strftime('%Y%m%d_%H:%M')
    __metadata__['run_id'] = run_id

    log_name = '{}-{}.log'.format(__metadata__['name'], run_id)
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(__metadata__),
                                        logger_name='wikipathways')

    main(write=not args.dummy, run_one=args.run_one)
