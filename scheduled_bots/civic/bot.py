import argparse
import copy
import json
import os
import traceback
from datetime import datetime

import pandas as pd
import requests
from tqdm import tqdm

from scheduled_bots import PROPS, ITEMS, get_default_core_props
from scheduled_bots.civic import CHROMOSOME, IGNORE_SYNONYMS, DrugCombo, EVIDENCE_LEVEL, TRUST_RATING
from wikidataintegrator import wdi_core, wdi_login, wdi_helpers
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

core_props = get_default_core_props()
core_props.update({'P3329'})

__metadata__ = {
    'name': 'ProteinBoxBot',
    'maintainer': 'Andra',
    'tags': ['variant'],
}

fast_run_base_filter = {'P3329': ''}
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


def create_reference(variant_id, retrieved):
    refStatedIn = wdi_core.WDItemID(value=ITEMS['CIViC database'], prop_nr=PROPS['stated in'], is_reference=True)
    timeStringNow = retrieved.strftime("+%Y-%m-%dT00:00:00Z")
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


def run_one(variant_id, retrieved, fast_run, write, login):
    variant_id = str(variant_id)
    r = requests.get('https://civic.genome.wustl.edu/api/variants/' + variant_id)
    variant_data = r.json()

    variant_reference = create_reference(variant_id, retrieved)

    prep = dict()

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
    if coordinates["chromosome"]:
        prep['P1057'] = [wdi_core.WDItemID(value=CHROMOSOME[coordinates["chromosome"]],
                                           prop_nr=PROPS['chromosome'],
                                           references=[variant_reference],
                                           qualifiers=[GENOME_BUILD_QUALIFIER])]
        if coordinates["chromosome2"]:
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

        if coordinates["start2"]:
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

    # only use evidence items that are accepted and have a rating
    evidence_items = [x for x in variant_data['evidence_items'] if
                      x["status"] == "accepted" and x["rating"] is not None]

    if not evidence_items:
        return

    evidence_statements = make_statements_from_evidences(variant_id, evidence_items, login, write)
    print(evidence_statements)

    data2add = []
    for key in prep.keys():
        for statement in prep[key]:
            data2add.append(statement)
    data2add.extend(evidence_statements)

    name = variant_data["name"]
    label = variant_data["entrez_name"] + " " + name
    # pprint([x.get_json_representation() for x in data2add])
    item = wdi_core.WDItemEngine(data=data2add, domain="genes", fast_run=fast_run, item_name=label,
                                 fast_run_base_filter=fast_run_base_filter, fast_run_use_refs=True,
                                 ref_handler=update_retrieved_if_new_multiple_refs, core_props=core_props)
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

    if write:
        try_write(item, record_id=variant_id, record_prop=PROPS['CIViC Variant ID'],
                  edit_summary="edit variant associations", login=login, write=write)


def is_valid_evidence_item(variant_id, evidence_item):
    # check to make sure evidence matches: https://civicdb.org/help/evidence/evidence-types
    # we'll also say No if the clinical_significance is "N/A" or None, bc we're not using it anyways
    allowed_evidence = {
        "Diagnostic": {"Negative", "Positive"},
        "Predictive": {"Resistance", "Sensitivity/Response"},
        "Prognostic": {"Better Outcome", "Good Outcome", "Poor Outcome"}
    }
    if evidence_item['evidence_type'] not in allowed_evidence.keys():
        panic(variant_id, "unknown evidence_type: {}".format(evidence_item['evidence_type']))
        return False
    if evidence_item['clinical_significance'] not in allowed_evidence[evidence_item['evidence_type']]:
        panic(variant_id, "unknown clinical_significance: {}".format(evidence_item['clinical_significance']))
        return False
    if evidence_item['evidence_direction'] not in {'Supports', 'Does Not Support'}:
        panic(variant_id, "unknown evidence_direction: {}".format(evidence_item['evidence_direction']))
        return False
    return True


def make_statements_from_evidences(variant_id, evidence_items, login, write):
    s = []

    for evidence_item in evidence_items:
        if not is_valid_evidence_item(variant_id, evidence_item):
            continue

        s.extend(make_statements_from_evidence(variant_id, evidence_item, login, write))

    return s


def make_statements_from_evidence(variant_id, evidence_item, login, write):
    ss = []
    ## determination method and rating qualifiers
    ev_quals = [wdi_core.WDItemID(value=EVIDENCE_LEVEL[str(evidence_item["evidence_level"])],
                                  prop_nr=PROPS['determination method'],
                                  is_qualifier=True),
                wdi_core.WDItemID(value=TRUST_RATING[str(evidence_item["rating"])],
                                  prop_nr=PROPS['rating'],
                                  is_qualifier=True)]

    ## Disease
    if not evidence_item["disease"]["doid"]:
        panic(variant_id, "", "no disease")
        return []
    doid = "DOID:" + evidence_item["disease"]["doid"]
    if doid not in DO_QID_MAP:
        panic(variant_id, doid, "disease")
        return []
    disease = DO_QID_MAP[doid]

    ## Drugs
    drug_qids = []
    for drug in evidence_item["drugs"]:
        drug_label = drug['name'].lower()
        if drug_label not in DRUGLABEL_QID_MAP:
            panic(variant_id, drug_label, "drug")
            return []
        drug_qids.append(DRUGLABEL_QID_MAP[drug_label])

    dit = evidence_item['drug_interaction_type']
    if dit == "Combination":
        # make this a drug therapy combination item instead!!
        drug_qids = [DrugCombo(drug_qids).get_or_create(login if write else None)]
    elif dit and dit not in {"Combination"}:
        # todo: Sequential, Substitutes
        panic(variant_id, "drug_interaction_type: {}".format(dit), "drug")
        return []

    ## Reference
    pmid = evidence_item["source"]["pubmed_id"]
    pmid_qid, _, _ = wdi_helpers.PublicationHelper(pmid.replace("PMID:", ""), id_type="pmid",
                                                   source="europepmc").get_or_create(login if write else None)
    if pmid_qid is None:
        panic(variant_id, "not found: {}".format(pmid), "pmid")
        return []
    refStatedIn = wdi_core.WDItemID(value=pmid_qid, prop_nr=PROPS['stated in'], is_reference=True)
    timeStringNow = retrieved.strftime("+%Y-%m-%dT00:00:00Z")
    refRetrieved = wdi_core.WDTime(timeStringNow, prop_nr=PROPS['retrieved'], is_reference=True)
    url = "https://civic.genome.wustl.edu/links/evidence/" + str(evidence_item['id'])
    refReferenceURL = wdi_core.WDUrl(url, prop_nr=PROPS['reference URL'], is_reference=True)
    refCurator = wdi_core.WDItemID(value=ITEMS['CIViC database'], prop_nr=PROPS['curator'], is_reference=True)
    evidence_reference = [refCurator, refRetrieved, refReferenceURL, refStatedIn]

    ## "disputed by" qualifer that can be added onto the qualifiers
    refDisputedBy = wdi_core.WDItemID(value=pmid_qid, prop_nr=PROPS['statement disputed by'], is_qualifier=True)

    # positive/negative therapeutic predictor
    if evidence_item["evidence_type"] == "Predictive":
        if evidence_item["clinical_significance"] == "Sensitivity/Response":
            prop = PROPS['positive therapeutic predictor']
        elif evidence_item["clinical_significance"] == "Resistance":
            prop = PROPS['negative therapeutic predictor']
        else:
            return []
        ev_quals.append(wdi_core.WDItemID(disease, PROPS['medical condition treated'], is_qualifier=True))
        if evidence_item["evidence_direction"] == "Does Not Support":
            ev_quals.append(refDisputedBy)
        for drug_qid in drug_qids:
            ss.append(wdi_core.WDItemID(drug_qid, prop, references=[evidence_reference], qualifiers=ev_quals))

    # positive/negative Diagnostic predictor
    if evidence_item["evidence_type"] == "Diagnostic":
        if evidence_item["clinical_significance"] == "Positive":
            prop = PROPS['positive diagnostic predictor']
        elif evidence_item["clinical_significance"] == "Negative":
            prop = PROPS['negative diagnostic predictor']
        else:
            return []
        if evidence_item["evidence_direction"] == "Does Not Support":
            ev_quals.append(refDisputedBy)
        ss.append(wdi_core.WDItemID(disease, prop, references=[evidence_reference], qualifiers=ev_quals))

    # positive/negative Prognostic predictor
    if evidence_item["evidence_type"] == "Prognostic":
        if evidence_item["clinical_significance"] in {"Better Outcome", "Good Outcome"}:
            prop = PROPS['positive prognostic predictor']
        elif evidence_item["clinical_significance"] == "Poor Outcome":
            prop = PROPS['negative prognostic predictor']
        else:
            return []
        if evidence_item["evidence_direction"] == "Does Not Support":
            ev_quals.append(refDisputedBy)
        ss.append(wdi_core.WDItemID(disease, prop, references=[evidence_reference], qualifiers=ev_quals))

    return ss


def main(retrieved, fast_run, write, variant_id=None):
    login = wdi_login.WDLogin(WDUSER, WDPASS)

    if variant_id:
        records = [{'id': variant_id}]
    else:
        r = requests.get('https://civic.genome.wustl.edu/api/variants?count=999999999')
        variants_data = r.json()
        records = variants_data['records']

    for record in tqdm(records):
        try:
            run_one(record['id'], retrieved, fast_run, write, login)
        except Exception as e:
            traceback.print_exc()
            wdi_core.WDItemEngine.log("ERROR", wdi_helpers.format_msg(
                record['id'], PROPS['CIViC Variant ID'], None, str(e), type(e)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='run civic bot')
    parser.add_argument('--dummy', help='do not actually do write', action='store_true')
    parser.add_argument('--no-fastrun', action='store_true')
    parser.add_argument('--variant-id', help="run only this one")
    args = parser.parse_args()
    log_dir = "./logs"
    run_id = datetime.now().strftime('%Y%m%d_%H:%M')
    __metadata__['run_id'] = run_id
    fast_run = False if args.no_fastrun else True
    retrieved = datetime.now()

    log_name = '{}-{}.log'.format(__metadata__['name'], run_id)
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(__metadata__),
                                        logger_name='civic')

    main(retrieved, fast_run=fast_run, write=not args.dummy, variant_id=args.variant_id)
