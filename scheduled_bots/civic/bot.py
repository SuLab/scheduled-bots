import sys
import os
from pprint import pprint

import pandas as pd
import requests

from scheduled_bots.civic import chromosomes, ignore_synonym_list, DrugCombo
from scheduled_bots.utils import get_values
from wikidataintegrator import wdi_core, wdi_login, wdi_property_store, wdi_helpers
from time import gmtime, strftime
import copy
from cachetools import cached, TTLCache

from wikidataintegrator.wdi_helpers import try_write

CACHE_SIZE = 10000
CACHE_TIMEOUT_SEC = 300  # 5 min

try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

PROPS = {
    'CIViC Variant ID': 'P3329',
    'instance of': 'P31',
    'stated in': 'P248',
    'reference URL': 'P854',
    'Entrez Gene ID ': 'P351',
    'found in taxon': 'P703',
    'biological variant of': 'P3433',
    'Sequence Ontology ID': 'P3986',
    'Disease Ontology ID': 'P699',
    'PubMed ID': 'P698'

}

ITEMS = {
    'CIViC database': 'Q27612411',
    'Homo sapiens': 'Q15978631'
}

# CIViC evidence scores
evidence_level = dict()
evidence_level["A"] = "Q36805652"
evidence_level["B"] = "Q36806012"
evidence_level["C"] = "Q36799701"
evidence_level["D"] = "Q36806470"
evidence_level["E"] = "Q36811327"

# CIViC trust ratings
trustratings = dict()
trustratings["1"] = "Q28045396"
trustratings["2"] = "Q28045397"
trustratings["3"] = "Q28045398"
trustratings["4"] = "Q28045383"
trustratings["5"] = "Q28045399"





wdi_property_store.wd_properties['P3329'] = {
    'datatype': 'string',
    'name': 'CIViC Variant ID',
    'domain': ['genes'],
    'core_id': True
}

__metadata__ = {
    'name': 'ProteinBoxBot',
    'maintainer': 'Andra',
    'tags': ['variant'],
    'properties': list(PROPS.values())
}

fast_run_base_filter = {'P3329': ''}
fast_run = True
GENOME_BUILD_QUALIFIER = wdi_core.WDItemID(value="Q21067546", prop_nr='P659', is_qualifier=True)
ENTREZ_QID_MAP = wdi_helpers.id_mapper(PROPS['Entrez Gene ID '], ((PROPS['found in taxon'], ITEMS['Homo sapiens']),))
SO_QID_MAP = wdi_helpers.id_mapper(PROPS['Sequence Ontology ID'])
DO_QID_MAP = wdi_helpers.id_mapper(PROPS['Disease Ontology ID'])


def load_drug_label_mappings():
    # we have a csv with drug label to qid mappings because civic doens't provide any mappings
    drugdf = pd.read_csv("drugs.tsv", sep='\t')
    return dict(zip(drugdf.name, drugdf.qid))

DRUGLABEL_QID_MAP = load_drug_label_mappings()

def create_reference(variant_id):
    refStatedIn = wdi_core.WDItemID(value=ITEMS['CIViC database'], prop_nr=PROPS['stated in'], is_reference=True)
    timeStringNow = strftime("+%Y-%m-%dT00:00:00Z", gmtime())
    refRetrieved = wdi_core.WDTime(timeStringNow, prop_nr='P813', is_reference=True)
    refReferenceURL = wdi_core.WDUrl("https://civic.genome.wustl.edu/links/variants/" + str(variant_id),
                                     prop_nr=PROPS['reference URL'],
                                     is_reference=True)
    variant_reference = [refStatedIn, refRetrieved, refReferenceURL]
    return variant_reference


def panic(variant_id, msg='', msg_type=''):
    msg = wdi_helpers.format_msg(variant_id, PROPS['CIViC Variant ID'], None, msg, msg_type)
    wdi_core.WDItemEngine.log("ERROR", msg)


@cached(TTLCache(CACHE_SIZE, CACHE_TIMEOUT_SEC))
def pmid_lookup(pmid):
    return wdi_helpers.prop2qid(prop=PROPS['PubMed ID'], value=pmid)


def run_one(variant_id):
    r = requests.get('https://civic.genome.wustl.edu/api/variants/' + str(variant_id))
    variant_data = r.json()

    variant_reference = create_reference(variant_id)

    prep = dict()

    entrez_id = variant_data["entrez_id"]
    if entrez_id not in ENTREZ_QID_MAP:
        return panic(variant_id)

    prep[PROPS['biological variant of']] = [wdi_core.WDItemID(value=ENTREZ_QID_MAP[entrez_id],
                                                              prop_nr=PROPS['biological variant of'],
                                                              references=[variant_reference])]

    # variant_id
    prep[PROPS['CIViC Variant ID']] = [
        wdi_core.WDString(value=variant_id, prop_nr='P3329', references=[copy.deepcopy(variant_reference)])]

    # coordinates
    coordinates = variant_data["coordinates"]
    if coordinates["chromosome"] != None:
        prep['P1057'] = [wdi_core.WDItemID(value=chromosomes[coordinates["chromosome"]], prop_nr='P1057',
                                           references=[copy.deepcopy(variant_reference)],
                                           qualifiers=[copy.deepcopy(GENOME_BUILD_QUALIFIER)])]
        if coordinates["chromosome2"] != None:
            prep['P1057'].append(wdi_core.WDItemID(value=chromosomes[coordinates["chromosome2"]], prop_nr='P1057',
                                                   references=[copy.deepcopy(variant_reference)],
                                                   qualifiers=[copy.deepcopy(GENOME_BUILD_QUALIFIER)]))

        # genomic start
        prep['P644'] = [wdi_core.WDString(value=str(coordinates["start"]), prop_nr='P644',
                                          references=[copy.deepcopy(variant_reference)],
                                          qualifiers=[copy.deepcopy(GENOME_BUILD_QUALIFIER)])]
        prep['P645'] = [wdi_core.WDString(value=str(coordinates["stop"]), prop_nr='P645',
                                          references=[copy.deepcopy(variant_reference)],
                                          qualifiers=[copy.deepcopy(GENOME_BUILD_QUALIFIER)])]

        if coordinates["start2"] != None:
            prep['P644'].append(wdi_core.WDString(value=str(coordinates["start2"]), prop_nr='P644',
                                                  references=[copy.deepcopy(variant_reference)],
                                                  qualifiers=[copy.deepcopy(GENOME_BUILD_QUALIFIER)]))
            prep['P645'].append(wdi_core.WDString(value=str(coordinates["stop2"]), prop_nr='P645',
                                                  references=[copy.deepcopy(variant_reference)],
                                                  qualifiers=[copy.deepcopy(GENOME_BUILD_QUALIFIER)]))

    # Sequence ontology variant_type = instance of
    prep["P31"] = []
    for variant_type in variant_data["variant_types"]:
        if variant_type["name"] == "N/A":
            continue
        prep['P31'].append(wdi_core.WDItemID(value=SO_QID_MAP[variant_type["so_id"]], prop_nr='P31',
                                             references=[copy.deepcopy(variant_reference)]))

    evidence = dict()
    evidence["P3354"] = dict()
    evidence["P3355"] = dict()
    evidence["P3356"] = dict()
    evidence["P3357"] = dict()
    evidence["P3358"] = dict()
    evidence["P3359"] = dict()
    for evidence_item in variant_data["evidence_items"]:
        if not evidence_item["disease"]["doid"]:
            continue
        disease = DO_QID_MAP[evidence_item["disease"]["doid"]]

        if evidence_item["status"] == "accepted" and evidence_item["rating"] != None:
            evidence_qualifiers = []
            evidence_qualifiers.append(
                wdi_core.WDItemID(value=evidence_level[str(evidence_item["evidence_level"])], prop_nr="P459",
                                  is_qualifier=True))
            evidence_qualifiers.append(
                wdi_core.WDItemID(value=trustratings[str(evidence_item["rating"])], prop_nr="P4271", is_qualifier=True))

            ## Drugs
            print(evidence_item["drugs"])
            drug_qids = []
            for drug in evidence_item["drugs"]:
                drug_label = drug['name'].lower()
                if drug_label not in DRUGLABEL_QID_MAP:
                    return panic(variant_id, drug_label, "drug not found")
                drug_qids.append(DRUGLABEL_QID_MAP[drug_label])

            if evidence_item['drug_interaction_type'] == "Combination":
                # make this a drug therapy combination item instead!!
                drug_qids = [DrugCombo(drug_qids).get_or_create(login)]
            else:
                drug = drug_qids[0]
            ## Pubmed
            evidence_reference = []
            pmid = evidence_item["source"]["pubmed_id"]
            pmid_qid = pmid_lookup(pmid)
            refStatedIn = wdi_core.WDItemID(value=pmid_qid, prop_nr='P248', is_reference=True)
            refDisputedBy = wdi_core.WDItemID(value=pmid_qid, prop_nr="P1310", is_qualifier=True)
            evidence_reference.append(refStatedIn)

            timeStringNow = strftime("+%Y-%m-%dT00:00:00Z", gmtime())
            refRetrieved = wdi_core.WDTime(timeStringNow, prop_nr='P813', is_reference=True)
            evidence_reference.append(refRetrieved)

            # Positive therapeutic predictor
            if evidence_item["evidence_type"] == "Predictive" and evidence_item[
                "clinical_significance"] == "Sensitivity" and evidence_item["evidence_direction"] == "Supports":
                temp_qualifier = [wdi_core.WDItemID(value=disease, prop_nr="P2175", is_qualifier=True)]
                for qualifier in evidence_qualifiers:
                    temp_qualifier.append(qualifier)
                evidence_qualifiers = temp_qualifier

                prep["P3354"].append(wdi_core.WDItemID(value=drug, prop_nr="P3354",
                                                       references=[copy.deepcopy(evidence_reference)],
                                                       qualifiers=copy.deepcopy(evidence_qualifiers)))

            # Evidence does not support Positive therapeutic predictor
            if evidence_item["evidence_type"] == "Predictive" and evidence_item[
                "clinical_significance"] == "Sensitivity" and evidence_item["evidence_direction"] == "Does Not Support":
                temp_qualifier = [wdi_core.WDItemID(value=disease, prop_nr="P2175", is_qualifier=True)]
                temp_qualifier.append(refDisputedBy)
                for qualifier in evidence_qualifiers:
                    temp_qualifier.append(qualifier)
                evidence_qualifiers = temp_qualifier
                prep["P3354"].append(wdi_core.WDItemID(value=drug, prop_nr="P3354",
                                                       references=[copy.deepcopy(evidence_reference)],
                                                       qualifiers=copy.deepcopy(evidence_qualifiers)))
            # Negative therapeutic predictor
            if evidence_item["evidence_type"] == "Resistance or Non-Response" and evidence_item[
                "clinical_significance"] == "Sensitivity" and evidence_item["evidence_direction"] == "Supports":
                temp_qualifier = [wdi_core.WDItemID(value=disease, prop_nr="P2175", is_qualifier=True)]
                for qualifier in evidence_qualifiers:
                    temp_qualifier.append(qualifier)
                evidence_qualifiers = temp_qualifier
                prep["P3355"].apeend(wdi_core.WDItemID(value=drug, prop_nr="P3355",
                                                       references=[copy.deepcopy(evidence_reference)],
                                                       qualifiers=copy.deepcopy(evidence_qualifiers)))

            # Evidence does not support Negative therapeutic predictor
            if evidence_item["evidence_type"] == "Predictive" and evidence_item[
                "clinical_significance"] == "Resistance or Non-Response" and evidence_item[
                "evidence_direction"] == "Does Not Support":
                temp_qualifier = [wdi_core.WDItemID(value=disease, prop_nr="P2175", is_qualifier=True)]
                temp_qualifier.append(refDisputedBy)
                for qualifier in evidence_qualifiers:
                    temp_qualifier.append(qualifier)
                evidence_qualifiers = temp_qualifier
                prep["P3355"].append(wdi_core.WDItemID(value=drug, prop_nr="P3355",
                                                       references=[copy.deepcopy(evidence_reference)],
                                                       qualifiers=copy.deepcopy(evidence_qualifiers)))
            # Positive diagnostic predictor
            if evidence_item["evidence_type"] == "Diagnostic" and evidence_item[
                "clinical_significance"] == "Sensitivity" and evidence_item["evidence_direction"] == "Supports":
                prep["P3356"].append(wdi_core.WDItemID(value=disease, prop_nr="P3356",
                                                       references=[copy.deepcopy(evidence_reference)],
                                                       qualifiers=evidence_qualifiers))
            # Evidence does not support Positive diagnostic predictor
            if evidence_item["evidence_type"] == "Diagnostic" and evidence_item[
                "clinical_significance"] == "Sensitivity" and evidence_item["evidence_direction"] == "Does Not Support":
                evidence_qualifiers.append(refDisputedBy)
                prep["P3356"].append(wdi_core.WDItemID(value=disease, prop_nr="P3356",
                                                       references=[copy.deepcopy(evidence_reference)],
                                                       qualifiers=copy.deepcopy(evidence_qualifiers)))
            # Negative diagnostic predictor
            if evidence_item["evidence_type"] == "Diagnostic" and evidence_item[
                "clinical_significance"] == "Resistance or Non-Response" and evidence_item[
                "evidence_direction"] == "Supports":
                prep["P3357"].append(wdi_core.WDItemID(value=disease, prop_nr="P3357",
                                                       references=[copy.deepcopy(evidence_reference)],
                                                       qualifiers=copy.deepcopy(evidence_qualifiers)))

            # Evidence does not support Negative diagnostic predictor
            if evidence_item["evidence_type"] == "Diagnostic" and evidence_item[
                "clinical_significance"] == "Resistance or Non-Response" and evidence_item[
                "evidence_direction"] == "Does Not Support":
                evidence_qualifiers.append(refDisputedBy)
                prep["P3357"].append(wdi_core.WDItemID(value=disease, prop_nr="P3357",
                                                       references=[copy.deepcopy(evidence_reference)],
                                                       qualifiers=copy.deepcopy(evidence_qualifiers)))

            # Positive prognostic predictor
            if evidence_item["evidence_type"] == "Prognositc" and evidence_item[
                "clinical_significance"] == "Sensitivity" and evidence_item["evidence_direction"] == "Supports":
                prep["P3358"].append(wdi_core.WDItemID(value=disease, prop_nr="P3358",
                                                       references=[copy.deepcopy(evidence_reference)],
                                                       qualifiers=copy.deepcopy(evidence_qualifiers)))

            # Evidence does not support Positive prognostic predictor
            if evidence_item["evidence_type"] == "Prognositc" and evidence_item[
                "clinical_significance"] == "Sensitivity" and evidence_item["evidence_direction"] == "Does Not Support":
                evidence_qualifiers.append(refDisputedBy)
                prep["P3358"].append(wdi_core.WDItemID(value=disease, prop_nr="P3358",
                                                       references=[copy.deepcopy(evidence_reference)],
                                                       qualifiers=copy.deepcopy(evidence_qualifiers)))

            # Negative prognostic predictor
            if evidence_item["evidence_type"] == "Prognostic" and evidence_item[
                "clinical_significance"] == "Resistance or Non-Response" and evidence_item[
                "evidence_direction"] == "Supports":
                prep["P3359"].append(wdi_core.WDItemID(value=disease, prop_nr="P3359",
                                                       references=[copy.deepcopy(evidence_reference)],
                                                       qualifiers=copy.deepcopy(evidence_qualifiers)))

            # Evidence does not support Negative prognostic predictor
            if evidence_item["evidence_type"] == "Prognostic" and evidence_item[
                "clinical_significance"] == "Resistance or Non-Response" and evidence_item[
                "evidence_direction"] == "Does Not Support":
                evidence_qualifiers.append(refDisputedBy)
                prep["P3359"].append(wdi_core.WDItemID(value=disease, prop_nr="P3359",
                                                       references=[copy.deepcopy(evidence_reference)],
                                                       qualifiers=copy.deepcopy(evidence_qualifiers)))

    data2add = []
    for key in prep.keys():
        for statement in prep[key]:
            data2add.append(statement)
            print(statement.prop_nr, statement.value)

    pprint(prep)
    name = variant_data["name"]
    item = wdi_core.WDItemEngine(data=data2add, domain="genes", fast_run=fast_run,
                                 fast_run_base_filter=fast_run_base_filter)
    synonyms = []
    if name not in ignore_synonym_list:
        synonyms.append(name)
    else:
        if name == "EXPRESSION":
            item.set_label("expressie van " + variant_data["entrez_name"], "nl")
        elif name == "BIALLELIC INACTIVATION":
            item.set_label("biallelische inactivatie van " + variant_data["entrez_name"], "nl")

    item.set_label(variant_data["entrez_name"] + " " + name, "en")

    if item.get_description(lang='en') == "":
        item.set_description("genetic variant", "en")
    if item.get_description(lang='nl') == "":
        item.set_description("gen variant", "nl")
    if len(variant_data["variant_aliases"]) > 0:
        for alias in variant_data["variant_aliases"]:
            synonyms.append(alias)
    if len(synonyms) > 0:
        item.set_aliases(aliases=synonyms, lang='en', append=True)

    try_write(item, record_id=variant_id, record_prop=PROPS['CIViC Variant ID'],
              edit_summary="edit variant associations", login=login)


if __name__ == "__main__":
    login = wdi_login.WDLogin(WDUSER, WDPASS)
    r = requests.get('https://civic.genome.wustl.edu/api/variants?count=999999999')
    variant_data = r.json()

    for record in variant_data['records']:
        # if record['id'] == 12:
        print(record['id'])
        run_one(record['id'])
