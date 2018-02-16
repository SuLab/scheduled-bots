"""
create pharmaceutical product

Example: https://www.wikidata.org/wiki/Q29004374
"""

import time
from collections import defaultdict

from wikidataintegrator import wdi_core, wdi_login, wdi_helpers, ref_handlers

from scheduled_bots.local import WDPASS, WDUSER


def make_ref(rxnorm):
    refs = [[
        wdi_core.WDItemID(value='Q7383767', prop_nr='P248', is_reference=True),  # stated in rxnorm
        wdi_core.WDExternalID(value=rxnorm, prop_nr='P3345', is_reference=True),  # rxcui
        wdi_core.WDTime(time=time.strftime('+%Y-%m-%dT00:00:00Z'), prop_nr='P813', is_reference=True)  # retrieved
        #wdi_core.WDTime(time=time.strftime('+2018-02-14T00:00:00Z'), prop_nr='P813', is_reference=True)  # retrieved
    ]]
    return refs


class Product:
    login = wdi_login.WDLogin(WDUSER, WDPASS)
    rxnorm_qid = wdi_helpers.id_mapper("P3345", return_as_set=True)
    rxnorm_qid = {k: list(v)[0] for k, v in rxnorm_qid.items() if len(v) == 1}
    qid_rxnorm = {v: k for k, v in rxnorm_qid.items()}

    def __init__(self, qid=None, rxcui=None, label=None):
        self.qid = qid
        self.rxcui = rxcui
        self.label = label
        if self.qid:
            # get the rxnorm id for this brand
            if rxcui and (self.qid_rxnorm[self.qid] != rxcui):
                raise ValueError("something is wrong: {}".format((self.qid, self.rxcui, rxcui)))
            self.rxcui = self.qid_rxnorm[self.qid]

    def add_active_ingredient(self, ingredient_qid):
        assert self.qid
        s = [wdi_core.WDItemID(ingredient_qid, 'P3781', references=make_ref(self.rxcui))]
        # purposely overwriting this
        item = wdi_core.WDItemEngine(wd_item_id=self.qid, data=s, domain="drugs",
                                     fast_run=True, fast_run_use_refs=True,
                                     fast_run_base_filter={"P3345": ""},
                                     ref_handler=ref_handlers.update_retrieved_if_new)
        item.write(self.login)

        # and adding the inverse
        s = [wdi_core.WDItemID(self.qid, 'P3780', references=make_ref(self.rxcui))]
        # do not overwrite
        item = wdi_core.WDItemEngine(wd_item_id=ingredient_qid, data=s, domain="drugs",
                                     fast_run=True, fast_run_use_refs=True,
                                     fast_run_base_filter={"P3345": ""},
                                     ref_handler=ref_handlers.update_retrieved_if_new,
                                     append_value=['P3780'])
        item.write(self.login)

    def get_or_create(self):
        assert self.rxcui
        if self.rxcui in self.rxnorm_qid:
            return self.rxnorm_qid[self.rxcui]
        assert self.label
        s = []
        s.append(wdi_core.WDItemID('Q28885102', 'P31', references=make_ref(self.rxcui)))  # pharma product
        s.append(wdi_core.WDExternalID(self.rxcui, "P3345", references=make_ref(self.rxcui)))

        item = wdi_core.WDItemEngine(item_name=self.label, data=s, domain="drugs")
        item.set_label(self.label)
        item.set_description("pharmaceutical product")
        item.write(self.login)
        qid = item.wd_item_id
        self.qid = qid
        return qid
