import argparse
import json
import os
from collections import defaultdict
from datetime import datetime
from itertools import chain
from time import gmtime, strftime
from tqdm import tqdm
from scheduled_bots import PROPS, ITEMS
from scheduled_bots.inxight.load_data import load_parsed_data
from wikidataintegrator import wdi_core, wdi_helpers, wdi_login
from wikidataintegrator.ref_handlers import update_retrieved_if_new_multiple_refs
from wikidataintegrator.wdi_helpers import try_write

__metadata__ = {
    'name': 'InxightBot',
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


class InxightBot:
    def __init__(self, drug_indications, login, write=True, run_one=False):
        self.drug_indications = drug_indications
        self.login = login
        self.write = write
        self.run_one = run_one

        self.core_props = set()
        self.append_props = [PROPS['drug used for treatment'], PROPS['medical condition treated']]

        # make the reverse mapping as well
        indication_drugs = defaultdict(list)
        for drug, indications in drug_indications.items():
            for ind in indications:
                indication_drugs[ind['indication_qid']].append(ind)
        self.indication_drugs = dict(indication_drugs)

        self.item_engine = self.make_item_engine()

        self.unii_qid = wdi_helpers.id_mapper(PROPS['UNII'])
        self.qid_unii = {v: k for k, v in self.unii_qid.items()}

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

    @staticmethod
    def create_reference(unii: str, url=None):
        """
        Reference is:
        retrieved: date
        stated in: links to pmid items
        optional reference URL
        """
        #
        ref = [wdi_core.WDItemID(ITEMS['Inxight: Drugs Database'], PROPS['stated in'], is_reference=True)]
        t = strftime("+%Y-%m-%dT00:00:00Z", gmtime())
        ref.append(wdi_core.WDTime(t, prop_nr=PROPS['retrieved'], is_reference=True))
        ref_url = "https://drugs.ncats.io/drug/{}".format(unii)
        ref.append(wdi_core.WDUrl(ref_url, PROPS['reference URL'], is_reference=True))
        if url:
            for u in url:
                try:
                    ref.append(wdi_core.WDUrl(u, PROPS['reference URL'], is_reference=True))
                except Exception as e:
                    print(e)
                    print(u)
        return ref

    @staticmethod
    def create_qualifier(start_time: datetime):
        q = []
        if start_time:
            q.append(
                wdi_core.WDTime(start_time.strftime('+%Y-%m-%dT00:00:00Z'), PROPS['start time'], is_qualifier=True))
        q.append(wdi_core.WDItemID(ITEMS['Food and Drug Administration'], PROPS['approved by'], is_qualifier=True))
        return q

    def run_one_drug(self, drug_qid, indications):
        ss = []
        unii = self.qid_unii[drug_qid]
        for ind in indications:
            refs = [self.create_reference(unii, ind['FdaUseURI'])]
            qual = self.create_qualifier(ind['ConditionProductDate'])
            s = wdi_core.WDItemID(ind['indication_qid'], PROPS['medical condition treated'],
                                  references=refs, qualifiers=qual)
            ss.append(s)

        item = self.item_engine(wd_item_id=drug_qid, data=ss)
        assert not item.create_new_item

        try_write(item, record_id=drug_qid, record_prop=PROPS['medical condition treated'],
                  edit_summary="Add indication from FDA", login=self.login, write=self.write)

    def run_one_indication(self, indication_qid, drugs):
        ss = []
        for drug in drugs:
            unii = self.qid_unii[drug]
            refs = [self.create_reference(unii, drug['FdaUseURI'])]
            qual = self.create_qualifier(drug['ConditionProductDate'])
            s = wdi_core.WDItemID(drug['drug_qid'], PROPS['drug used for treatment'],
                                  references=refs, qualifiers=qual)
            ss.append(s)

        item = self.item_engine(wd_item_id=indication_qid, data=ss)
        assert not item.create_new_item

        try_write(item, record_id=indication_qid, record_prop=PROPS['drug used for treatment'],
                  edit_summary="Add indication from FDA", login=self.login, write=self.write)

    def run(self):
        if self.run_one:
            if self.run_one in self.drug_indications:
                print(self.drug_indications[self.run_one])
                self.run_one_drug(self.run_one, self.drug_indications[self.run_one])
            elif self.run_one in self.indication_drugs:
                self.run_one_indication(self.run_one, self.indication_drugs[self.run_one])
            else:
                raise ValueError("{} not found".format(self.run_one))
            return None
        d = sorted(self.drug_indications.items(), key=lambda x: x[0])
        for drug_qid, indications_qid in tqdm(d):
            self.run_one_drug(drug_qid, indications_qid)
        d = sorted(self.indication_drugs.items(), key=lambda x: x[0])
        for indication_qid, drugs_qid in tqdm(d):
            self.run_one_indication(indication_qid, drugs_qid)


def normalize_to_qids(d: dict):
    chembl_qid = wdi_helpers.id_mapper(PROPS['ChEMBL ID'])
    doid_qid = wdi_helpers.id_mapper(PROPS['Disease Ontology ID'])

    d = {chembl_qid.get(k): d[k] for k in d.keys()}

    for ind in chain(*d.values()):
        ind['indication_qid'] = doid_qid.get(ind['ConditionDoId'])
    for key in d:
        d[key] = [x for x in d[key] if x['indication_qid']]
        for x in d[key]:
            x['drug_qid'] = key
    d = {k: v for k, v in d.items() if k and v}
    return d


def main(write=True, run_one=None):
    d = load_parsed_data()
    login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS)

    drug_indications = normalize_to_qids(d)

    bot = InxightBot(drug_indications, login, write=write, run_one=run_one)
    bot.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='run inxight indications bot')
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
                                        logger_name='inxight')

    main(write=not args.dummy, run_one=args.run_one)
