import argparse
import json
import os
from datetime import datetime
from itertools import groupby
from time import gmtime, strftime, strptime

import pandas as pd
from tqdm import tqdm

from scheduled_bots import PROPS, ITEMS
from wikidataintegrator import wdi_core, wdi_helpers, wdi_login
from wikidataintegrator.ref_handlers import update_retrieved_if_new_multiple_refs
from wikidataintegrator.wdi_helpers import PublicationHelper
from wikidataintegrator.wdi_helpers import try_write

__metadata__ = {
    'name': 'MitoBot',
    'maintainer': 'GSS',
    'tags': ['disease', 'phenotype'],
    'properties': [PROPS['symptoms']]
}
try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")


class MitoBot:
    def __init__(self, records, login, write=True, run_one=False):
        """
        # records is a list of dicts that look like:
        {'Added on(yyyy-mm-dd)': '2011-10-27',
         'Organ system': 'nervous',
         'Percent affected': '100 %',
         'Pubmed id': 19696032,
         'Symptom/sign': 'ataxia',
         'disease': 606002,
         'hpo': 'HP:0001251'}
        """
        self.records = records
        self.login = login
        self.write = write
        self.run_one = run_one
        self.core_props = set()
        self.append_props = [PROPS['symptoms']]
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

    @staticmethod
    def create_reference(pmid, login=None):
        """
        Reference is:
        retrieved: date
        stated in: links to pmid items
        optional reference URL
        """
        #
        ref = [wdi_core.WDItemID(ITEMS['MitoDB'], PROPS['curator'], is_reference=True)]
        t = strftime("+%Y-%m-%dT00:00:00Z", gmtime())
        ref.append(wdi_core.WDTime(t, prop_nr=PROPS['retrieved'], is_reference=True))
        pmid_qid, _, success = PublicationHelper(ext_id=pmid, id_type='pmid', source="europepmc").get_or_create(login)
        if success:
            ref.append(wdi_core.WDItemID(pmid_qid, PROPS['stated in'], is_reference=True))
        return ref

    @staticmethod
    def create_qualifier(incidence):
        q = []
        if incidence:
            q.append(wdi_core.WDQuantity(incidence, PROPS['incidence'], is_qualifier=True,
                                         unit="http://www.wikidata.org/entity/" + ITEMS['percentage']))
            pass
        return q

    def run_one_disease(self, disease_qid, records):
        ss = []
        for record in records:
            incidence = float(record['Percent affected'][:-2])
            pmid = record['Pubmed id']
            phenotype_qid = record['phenotype_qid']

            refs = [self.create_reference(pmid=pmid, login=self.login)]
            qual = self.create_qualifier(incidence)
            s = wdi_core.WDItemID(phenotype_qid, PROPS['symptoms'], references=refs, qualifiers=qual)
            ss.append(s)

        item = self.item_engine(wd_item_id=disease_qid, data=ss)
        assert not item.create_new_item

        try_write(item, record_id=disease_qid, record_prop=PROPS['symptoms'],
                  edit_summary="Add phenotype from mitodb", login=self.login, write=self.write)

    def run(self):
        if self.run_one:
            d = [x for x in self.records if x['disease_qid'] == self.run_one]
            if d:
                print(d[0])
                self.run_one_disease(d[0]['disease_qid'], d)
            else:
                raise ValueError("{} not found".format(self.run_one))
            return None
        self.records = sorted(self.records, key=lambda x: x['disease_qid'])
        record_group = groupby(self.records, key=lambda x: x['disease_qid'])
        for disease_qid, sub_records in tqdm(record_group):
            self.run_one_disease(disease_qid, sub_records)


def main(write=True, run_one=None):
    omim_qid = wdi_helpers.id_mapper(PROPS['OMIM ID'], prefer_exact_match=True)
    hpo_qid = wdi_helpers.id_mapper(PROPS['Human Phenotype Ontology ID'], prefer_exact_match=True)

    df = pd.read_csv("mitodb.csv", dtype=str)
    df['disease_qid'] = df.disease.map(omim_qid.get)
    df['phenotype_qid'] = df.hpo.map(hpo_qid.get)
    df.dropna(subset=['disease_qid', 'phenotype_qid'], inplace=True)

    records = df.to_dict("records")
    login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS)
    write = True
    run_one = "Q55345782"
    bot = MitoBot(records, login, write=write, run_one=run_one)
    bot.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='run mitodb phenotype bot')
    parser.add_argument('--dummy', help='do not actually do write', action='store_true')
    parser.add_argument('--run-one', help='run one disease, by qid')
    args = parser.parse_args()
    log_dir = "./logs"
    run_id = datetime.now().strftime('%Y%m%d_%H:%M')
    __metadata__['run_id'] = run_id

    log_name = '{}-{}.log'.format(__metadata__['name'], run_id)
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(__metadata__),
                                        logger_name='mitodb')

    main(write=not args.dummy, run_one=args.run_one)
