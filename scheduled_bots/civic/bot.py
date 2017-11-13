import copy
import os
from pprint import pprint
from time import gmtime, strftime

import pandas as pd
import requests
from cachetools import cached, TTLCache

from scheduled_bots.civic import CHROMOSOME, IGNORE_SYNONYMS, DrugCombo, EVIDENCE_LEVEL, TRUST_RATING
from wikidataintegrator import wdi_core, wdi_login, wdi_property_store, wdi_helpers
from wikidataintegrator.ref_handlers import update_retrieved_if_new_multiple_refs
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
    'PubMed ID': 'P698',
    'positive therapeutic predictor': 'P3354',
    'negative therapeutic predictor': 'P3355',
    'positive diagnostic predictor': 'P3356',
    'negative diagnostic predictor': 'P3357',
    'positive prognostic predictor': 'P3358',
    'negative prognostic predictor': 'P3359',
    'HGVS nomenclature': 'P3331',
    'chromosome': 'P1057',
    'genomic start': 'P644',
    'genomic end': 'P645',
    'determination method': 'P459',
    'rating': 'P4271',
    'medical condition treated': 'P2175',
    'curator': 'P1640',
    'statement disputed by': 'P1310',
    'retrieved': 'P813'
}

ITEMS = {
    'CIViC database': 'Q27612411',
    'Homo sapiens': 'Q15978631'
}

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
    # we have a csv with drug label to qid mappings because civic doesn't provide any mappings
    drugdf = pd.read_csv("drugs.tsv", sep='\t')
    d = dict(zip(drugdf.name, drugdf.qid))
    # remove the nan ones
    d = {k: v for k, v in d.items() if isinstance(v, str)}
    return d


DRUGLABEL_QID_MAP = load_drug_label_mappings()


def create_reference(variant_id):
    refStatedIn = wdi_core.WDItemID(value=ITEMS['CIViC database'], prop_nr=PROPS['stated in'], is_reference=True)
    timeStringNow = strftime("+%Y-%m-%dT00:00:00Z", gmtime())
    refRetrieved = wdi_core.WDTime(timeStringNow, prop_nr=PROPS['retrieved'], is_reference=True)
    refReferenceURL = wdi_core.WDUrl("https://civic.genome.wustl.edu/links/variants/" + str(variant_id),
                                     prop_nr=PROPS['reference URL'],
                                     is_reference=True)
    variant_reference = [refStatedIn, refRetrieved, refReferenceURL]
    return variant_reference


def panic(variant_id, msg='', msg_type=''):
    s = wdi_helpers.format_msg(variant_id, PROPS['CIViC Variant ID'], None, msg, msg_type)
    wdi_core.WDItemEngine.log("ERROR", s)
    print(s)
    return None


@cached(TTLCache(CACHE_SIZE, CACHE_TIMEOUT_SEC))
def pmid_lookup(pmid):
    return wdi_helpers.prop2qid(prop=PROPS['PubMed ID'], value=pmid)


def run_one(variant_id):
    variant_id = str(variant_id)
    r = requests.get('https://civic.genome.wustl.edu/api/variants/' + variant_id)
    variant_data = r.json()

    variant_reference = create_reference(variant_id)

    prep = {
        PROPS['positive therapeutic predictor']: list(),
        PROPS['positive diagnostic predictor']: list(),
        PROPS['positive prognostic predictor']: list(),
        PROPS['negative therapeutic predictor']: list(),
        PROPS['negative diagnostic predictor']: list(),
        PROPS['negative prognostic predictor']: list(),
    }

    entrez_id = str(variant_data["entrez_id"])
    if entrez_id not in ENTREZ_QID_MAP:
        return panic(variant_id, msg=entrez_id, msg_type="Entrez ID not found")

    prep[PROPS['biological variant of']] = [wdi_core.WDItemID(value=ENTREZ_QID_MAP[entrez_id],
                                                              prop_nr=PROPS['biological variant of'],
                                                              references=[variant_reference])]

    # variant_id
    prep[PROPS['CIViC Variant ID']] = [
        wdi_core.WDString(value=variant_id, prop_nr=PROPS['CIViC Variant ID'], references=[variant_reference])]

    # hgvs ids
    prep[PROPS['HGVS nomenclature']] = []
    for hgvs in variant_data['hgvs_expressions']:
        prep[PROPS['HGVS nomenclature']].append(
            wdi_core.WDString(value=hgvs, prop_nr=PROPS['HGVS nomenclature'], references=[variant_reference]))

    # coordinates
    coordinates = variant_data["coordinates"]
    if coordinates["chromosome"] != None:
        prep['P1057'] = [wdi_core.WDItemID(value=CHROMOSOME[coordinates["chromosome"]],
                                           prop_nr=PROPS['chromosome'],
                                           references=[variant_reference],
                                           qualifiers=[GENOME_BUILD_QUALIFIER])]
        if coordinates["chromosome2"] != None:
            prep['P1057'].append(wdi_core.WDItemID(value=CHROMOSOME[coordinates["chromosome2"]],
                                                   prop_nr=PROPS['chromosome'],
                                                   references=[variant_reference],
                                                   qualifiers=[GENOME_BUILD_QUALIFIER]))

        # genomic start
        prep['P644'] = [wdi_core.WDString(value=str(coordinates["start"]),
                                          prop_nr=PROPS['genomic start'],
                                          references=[variant_reference],
                                          qualifiers=[GENOME_BUILD_QUALIFIER])]
        prep['P645'] = [wdi_core.WDString(value=str(coordinates["stop"]),
                                          prop_nr=PROPS['genomic end'],
                                          references=[variant_reference],
                                          qualifiers=[GENOME_BUILD_QUALIFIER])]

        if coordinates["start2"] != None:
            prep['P644'].append(wdi_core.WDString(value=str(coordinates["start2"]),
                                                  prop_nr=PROPS['genomic start'],
                                                  references=[variant_reference],
                                                  qualifiers=[GENOME_BUILD_QUALIFIER]))
            prep['P645'].append(wdi_core.WDString(value=str(coordinates["stop2"]),
                                                  prop_nr=PROPS['genomic end'],
                                                  references=[variant_reference],
                                                  qualifiers=[GENOME_BUILD_QUALIFIER]))

    # Sequence ontology variant_type = instance of
    prep["P31"] = []
    for variant_type in variant_data["variant_types"]:
        if variant_type["name"] == "N/A":
            prep['P31'].append(wdi_core.WDItemID(value="Q15304597", prop_nr='P31',
                                                 references=[variant_reference]))
        else:
            prep['P31'].append(wdi_core.WDItemID(value=SO_QID_MAP[variant_type["so_id"]], prop_nr='P31',
                                                 references=[variant_reference]))

    evidence = dict()
    evidence["P3354"] = dict()
    evidence["P3355"] = dict()
    evidence["P3356"] = dict()
    evidence["P3357"] = dict()
    evidence["P3358"] = dict()
    evidence["P3359"] = dict()

    # only use evidence items that are accepted and have a rating
    evidence_items = [x for x in variant_data['evidence_items'] if
                      x["status"] == "accepted" and x["rating"] is not None]

    if not evidence_items:
        return

    for evidence_item in evidence_items:
        pprint(evidence_item)

        ## determination method and rating qualifiers
        evidence_qualifiers = [wdi_core.WDItemID(value=EVIDENCE_LEVEL[str(evidence_item["evidence_level"])],
                                                 prop_nr=PROPS['determination method'],
                                                 is_qualifier=True),
                               wdi_core.WDItemID(value=TRUST_RATING[str(evidence_item["rating"])],
                                                 prop_nr=PROPS['rating'],
                                                 is_qualifier=True)]

        ## Disease
        if not evidence_item["disease"]["doid"]:
            continue
        doid = "DOID:" + evidence_item["disease"]["doid"]
        if doid not in DO_QID_MAP:
            return panic(variant_id, doid, "disease not found")
        disease = DO_QID_MAP[doid]

        ## Drugs
        drug_qids = []
        for drug in evidence_item["drugs"]:
            drug_label = drug['name'].lower()
            if drug_label not in DRUGLABEL_QID_MAP:
                return panic(variant_id, drug_label, "drug not found")
            drug_qids.append(DRUGLABEL_QID_MAP[drug_label])

        if evidence_item['drug_interaction_type'] == "Combination":
            # make this a drug therapy combination item instead!!
            drug_qids = [DrugCombo(drug_qids).get_or_create(login)]

        # TODO: "substitution"
        print("drug_qids: {}".format(drug_qids))

        ## Reference
        pmid = evidence_item["source"]["pubmed_id"]
        pmid_qid = pmid_lookup(pmid)
        refStatedIn = wdi_core.WDItemID(value=pmid_qid, prop_nr=PROPS['stated in'], is_reference=True)
        timeStringNow = strftime("+%Y-%m-%dT00:00:00Z", gmtime())
        refRetrieved = wdi_core.WDTime(timeStringNow, prop_nr=PROPS['retrieved'], is_reference=True)
        url = "https://civic.genome.wustl.edu/links/evidence/" + str(evidence_item['id'])
        refReferenceURL = wdi_core.WDUrl(url, prop_nr=PROPS['reference URL'], is_reference=True)
        refCurator = wdi_core.WDItemID(value=ITEMS['CIViC database'], prop_nr=PROPS['curator'], is_reference=True)
        evidence_reference = [refCurator, refRetrieved, refReferenceURL, refStatedIn]

        ## "disputed by" qualifer that can be added onto the qualifiers
        refDisputedBy = wdi_core.WDItemID(value=pmid_qid, prop_nr=PROPS['statement disputed by'], is_qualifier=True)

        # Positive therapeutic predictor
        if evidence_item["evidence_type"] == "Predictive" and evidence_item[
            "clinical_significance"] == "Sensitivity" and evidence_item["evidence_direction"] == "Supports":
            temp_qualifier = [
                wdi_core.WDItemID(value=disease, prop_nr=PROPS['medical condition treated'], is_qualifier=True)]
            for qualifier in evidence_qualifiers:
                temp_qualifier.append(qualifier)
            evidence_qualifiers = temp_qualifier
            for drug_qid in drug_qids:
                prep["P3354"].append(wdi_core.WDItemID(value=drug_qid, prop_nr=PROPS['positive therapeutic predictor'],
                                                       references=[copy.deepcopy(evidence_reference)],
                                                       qualifiers=copy.deepcopy(evidence_qualifiers)))

        # Evidence does not support Positive therapeutic predictor
        if evidence_item["evidence_type"] == "Predictive" and evidence_item[
            "clinical_significance"] == "Sensitivity" and evidence_item["evidence_direction"] == "Does Not Support":
            temp_qualifier = [
                wdi_core.WDItemID(value=disease, prop_nr=PROPS['medical condition treated'], is_qualifier=True)]
            temp_qualifier.append(refDisputedBy)
            for qualifier in evidence_qualifiers:
                temp_qualifier.append(qualifier)
            evidence_qualifiers = temp_qualifier
            for drug_qid in drug_qids:
                prep["P3354"].append(wdi_core.WDItemID(value=drug_qid, prop_nr=PROPS['positive therapeutic predictor'],
                                                       references=[copy.deepcopy(evidence_reference)],
                                                       qualifiers=copy.deepcopy(evidence_qualifiers)))
        # Negative therapeutic predictor
        if evidence_item["evidence_type"] == "Resistance or Non-Response" and evidence_item[
            "clinical_significance"] == "Sensitivity" and evidence_item["evidence_direction"] == "Supports":
            temp_qualifier = [
                wdi_core.WDItemID(value=disease, prop_nr=PROPS['medical condition treated'], is_qualifier=True)]
            for qualifier in evidence_qualifiers:
                temp_qualifier.append(qualifier)
            evidence_qualifiers = temp_qualifier
            for drug_qid in drug_qids:
                prep["P3355"].append(wdi_core.WDItemID(value=drug_qid, prop_nr=PROPS['negative therapeutic predictor'],
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
            for drug_qid in drug_qids:
                prep["P3355"].append(wdi_core.WDItemID(value=drug_qid, prop_nr=PROPS['negative therapeutic predictor'],
                                                       references=[copy.deepcopy(evidence_reference)],
                                                       qualifiers=copy.deepcopy(evidence_qualifiers)))
        # Positive diagnostic predictor
        if evidence_item["evidence_type"] == "Diagnostic" and evidence_item[
            "clinical_significance"] == "Sensitivity" and evidence_item["evidence_direction"] == "Supports":
            prep["P3356"].append(wdi_core.WDItemID(value=disease, prop_nr=PROPS['positive diagnostic predictor'],
                                                   references=[copy.deepcopy(evidence_reference)],
                                                   qualifiers=evidence_qualifiers))
        # Evidence does not support Positive diagnostic predictor
        if evidence_item["evidence_type"] == "Diagnostic" and evidence_item[
            "clinical_significance"] == "Sensitivity" and evidence_item["evidence_direction"] == "Does Not Support":
            evidence_qualifiers.append(refDisputedBy)
            prep["P3356"].append(wdi_core.WDItemID(value=disease, prop_nr=PROPS['positive diagnostic predictor'],
                                                   references=[copy.deepcopy(evidence_reference)],
                                                   qualifiers=copy.deepcopy(evidence_qualifiers)))
        # Negative diagnostic predictor
        if evidence_item["evidence_type"] == "Diagnostic" and evidence_item[
            "clinical_significance"] == "Resistance or Non-Response" and evidence_item[
            "evidence_direction"] == "Supports":
            prep["P3357"].append(wdi_core.WDItemID(value=disease, prop_nr=PROPS['negative diagnostic predictor'],
                                                   references=[copy.deepcopy(evidence_reference)],
                                                   qualifiers=copy.deepcopy(evidence_qualifiers)))

        # Evidence does not support Negative diagnostic predictor
        if evidence_item["evidence_type"] == "Diagnostic" and evidence_item[
            "clinical_significance"] == "Resistance or Non-Response" and evidence_item[
            "evidence_direction"] == "Does Not Support":
            evidence_qualifiers.append(refDisputedBy)
            prep["P3357"].append(wdi_core.WDItemID(value=disease, prop_nr=PROPS['negative diagnostic predictor'],
                                                   references=[copy.deepcopy(evidence_reference)],
                                                   qualifiers=copy.deepcopy(evidence_qualifiers)))

        # Positive prognostic predictor
        if evidence_item["evidence_type"] == "Prognositc" and evidence_item[
            "clinical_significance"] == "Sensitivity" and evidence_item["evidence_direction"] == "Supports":
            prep["P3358"].append(wdi_core.WDItemID(value=disease, prop_nr=PROPS['positive prognostic predictor'],
                                                   references=[copy.deepcopy(evidence_reference)],
                                                   qualifiers=copy.deepcopy(evidence_qualifiers)))

        # Evidence does not support Positive prognostic predictor
        if evidence_item["evidence_type"] == "Prognositc" and evidence_item[
            "clinical_significance"] == "Sensitivity" and evidence_item["evidence_direction"] == "Does Not Support":
            evidence_qualifiers.append(refDisputedBy)
            prep["P3358"].append(wdi_core.WDItemID(value=disease, prop_nr=PROPS['positive prognostic predictor'],
                                                   references=[copy.deepcopy(evidence_reference)],
                                                   qualifiers=copy.deepcopy(evidence_qualifiers)))

        # Negative prognostic predictor
        if evidence_item["evidence_type"] == "Prognostic" and evidence_item[
            "clinical_significance"] == "Resistance or Non-Response" and evidence_item[
            "evidence_direction"] == "Supports":
            prep["P3359"].append(wdi_core.WDItemID(value=disease, prop_nr=PROPS['negative prognostic predictor'],
                                                   references=[copy.deepcopy(evidence_reference)],
                                                   qualifiers=copy.deepcopy(evidence_qualifiers)))

        # Evidence does not support Negative prognostic predictor
        if evidence_item["evidence_type"] == "Prognostic" and evidence_item[
            "clinical_significance"] == "Resistance or Non-Response" and evidence_item[
            "evidence_direction"] == "Does Not Support":
            evidence_qualifiers.append(refDisputedBy)
            prep["P3359"].append(wdi_core.WDItemID(value=disease, prop_nr=PROPS['negative prognostic predictor'],
                                                   references=[copy.deepcopy(evidence_reference)],
                                                  qualifiers=copy.deepcopy(evidence_qualifiers)))
    dowrite = False
    data2add = []
    for key in prep.keys():
        if key in ["P3354", "P3355", "P3356", "P3357", "P3358", "P3359"]:
            dowrite = True
        for statement in prep[key]:
            data2add.append(statement)
            print(statement.prop_nr, statement.value)

    pprint(prep)
    name = variant_data["name"]
    label = variant_data["entrez_name"] + " " + name
    item = wdi_core.WDItemEngine(data=data2add, domain="genes", fast_run=fast_run, item_name=label,
                                 fast_run_base_filter=fast_run_base_filter, fast_run_use_refs=True,
                                 ref_handler=update_retrieved_if_new_multiple_refs)
    synonyms = []
    if name not in IGNORE_SYNONYMS:
        synonyms.append(name)
    else:
        if name == "EXPRESSION":
            item.set_label("expressie van " + variant_data["entrez_name"], "nl")
        elif name == "BIALLELIC INACTIVATION":
            item.set_label("biallelische inactivatie van " + variant_data["entrez_name"], "nl")

    item.set_label(label, "en")

    if item.get_description(lang='en') == "":
        item.set_description("genetic variant", "en")
    if item.get_description(lang='nl') == "":
        item.set_description("gen variant", "nl")
    if len(variant_data["variant_aliases"]) > 0:
        for alias in variant_data["variant_aliases"]:
            synonyms.append(alias)
    if len(synonyms) > 0:
        item.set_aliases(aliases=synonyms, lang='en', append=True)

    if dowrite:
        try_write(item, record_id=variant_id, record_prop=PROPS['CIViC Variant ID'],
                  edit_summary="edit variant associations", login=login)


if __name__ == "__main__":
    login = wdi_login.WDLogin(WDUSER, WDPASS)
    r = requests.get('https://civic.genome.wustl.edu/api/variants?count=999999999')
    variants_data = r.json()

    for record in variants_data['records']:
            # if record['id'] == 98:
            print(record['id'])
            try:
                run_one(record['id'])
            except Exception as e:
                print(e)
                wdi_core.WDItemEngine.log("ERROR", wdi_helpers.format_msg(
                    record['id'], PROPS['CIViC Variant ID'], None, str(e), type(e)))
