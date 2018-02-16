"""
create drug/product mixtures

Example: https://www.wikidata.org/wiki/Q4663143
"""
import time
from collections import defaultdict

from wikidataintegrator import wdi_core, wdi_login, wdi_helpers

from scheduled_bots.local import WDPASS, WDUSER


def make_ref(rxnorm):
    refs = [[
        wdi_core.WDItemID(value='Q7383767', prop_nr='P248', is_reference=True),  # stated in rxnorm
        wdi_core.WDExternalID(value=rxnorm, prop_nr='P3345', is_reference=True),  # rxcui
        wdi_core.WDTime(time=time.strftime('+%Y-%m-%dT00:00:00Z'), prop_nr='P813', is_reference=True)  # retrieved
    ]]
    return refs


class Mixtures:

    def __init__(self):
        self.login = wdi_login.WDLogin(WDUSER, WDPASS)
        self._get_mixtures_in_wd()

        rxnorm_qid = wdi_helpers.id_mapper("P3345", return_as_set=True)
        rxnorm_qid = {k: list(v)[0] for k, v in rxnorm_qid.items() if len(v) == 1}
        self.rxnorm_qid = rxnorm_qid

    def _get_mixtures_in_wd(self):
        query = """
        SELECT distinct ?drug ?compound WHERE {
            values ?chemical {wd:Q12140 wd:Q11173 wd:Q79529}
            ?drug wdt:P527 ?compound .
            ?drug wdt:P31 ?chemical .
            ?compound wdt:P652 ?unii
        }"""
        mixd = defaultdict(set)
        r = wdi_core.WDItemEngine.execute_sparql_query(query=query)
        for x in r['results']['bindings']:
            parent = x['drug']['value'].split("/")[-1]
            mixd[parent].add(x['compound']['value'].split("/")[-1])
        self.mixture_components = {k: v for k, v in mixd.items() if len(v) > 1}
        self.components_mixture = {frozenset(v): k for k, v in self.mixture_components.items()}

    # to create, needs: label, ingredients, rxcui
    def create(self, label: str, rxcui: str, ingredient_qids: list):
        rxcui = str(rxcui)
        # check to make sure it doesn't exist
        if rxcui in self.rxnorm_qid:
            raise ValueError("rxcui {} already exists: {}".format(rxcui, self.rxnorm_qid[rxcui]))
        # check by ingredients
        qid = self.get_mixture_qid(ingredient_qids)
        if qid:
            raise ValueError("mixture already exists: {}".format(qid))

        # has part
        s = [wdi_core.WDItemID(x, 'P527', references=make_ref(rxcui)) for x in ingredient_qids]
        # instance of
        s.append(wdi_core.WDItemID('Q12140', 'P31', references=make_ref(rxcui)))  # drug
        s.append(wdi_core.WDItemID('Q79529', 'P31', references=make_ref(rxcui)))  # chemical substance
        s.append(wdi_core.WDItemID('Q169336', 'P31', references=make_ref(rxcui)))  # mixture
        # rxnorm
        s.append(wdi_core.WDExternalID(rxcui, "P3345", references=make_ref(rxcui)))

        item = wdi_core.WDItemEngine(item_name=label, data=s, domain="drugs")
        if item.create_new_item:
            item.set_label(label)
        item.set_label(label)
        if not item.get_description():
            item.set_description("combination drug")
        item.write(self.login)
        qid = item.wd_item_id

        # update cache
        self.components_mixture[frozenset(ingredient_qids)] = qid
        self.mixture_components[qid] = ingredient_qids
        self.rxnorm_qid[rxcui] = qid

        return qid

    def get_or_create(self, label, rxcui, ingredient_qids):
        if rxcui in self.rxnorm_qid:
            return self.rxnorm_qid[rxcui]
        qid = self.get_mixture_qid(ingredient_qids)
        if qid:
            return qid
        return self.create(label, rxcui, ingredient_qids)


    def get_mixture_qid(self, ingredient_qids):
        # get the qid for the mixture from the ingredient qids
        return self.components_mixture.get(frozenset(ingredient_qids))
