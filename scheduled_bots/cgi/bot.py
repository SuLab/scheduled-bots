import argparse
import json
import os
from datetime import datetime
from time import gmtime, strftime
from urllib.parse import quote

import myvariant
import pandas as pd
from tqdm import tqdm

from scheduled_bots import PROPS, ITEMS, get_default_core_props
from scheduled_bots.geneprotein import human_chromosome_map
from wikidataintegrator import wdi_core, wdi_helpers, wdi_login
from wikidataintegrator.ref_handlers import update_retrieved_if_new_multiple_refs
from wikidataintegrator.wdi_helpers import id_mapper

try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

## First check if combinatorial therapies have duplicates on Wikidata and merge them before continuing
# get existing combinations:
query_str = """SELECT ?item ?itemLabel (GROUP_CONCAT(?part; separator=";") as ?f) WHERE {
  ?item wdt:P527 ?part .
  ?item wdt:P31 wd:Q1304270 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
} GROUP BY ?item ?itemLabel"""
results = wdi_core.WDItemEngine.execute_sparql_query(query_str)['results']['bindings']
combo_qid = {x['item']['value'].replace("http://www.wikidata.org/entity/", ""): frozenset([y.replace("http://www.wikidata.org/entity/", "") for y in x['f']['value'].split(";")]) for x in results}
qid_combo = {v:k for k,v in combo_qid.items()}

login = wdi_login.WDLogin(WDUSER, WDPASS)
solved =[]
for qid1 in combo_qid.keys():
    if qid1 in solved:
        continue
    for qid2 in combo_qid.keys():
        if qid1 != qid2:
            if combo_qid[qid1] == combo_qid[qid2]:
                print(qid1, combo_qid[qid1], ":", qid2, combo_qid[qid2])
                print(qid1[1:])
                if int(qid1[1:]) > int(qid2[1:]):
                    source = qid2
                    target = qid1
                else:
                    source = qid2
                    target = qid1
                wdi_core.WDItemEngine.merge_items(source, target, login)
                solved.append(qid1)
                solved.append(qid2)

# map association column
association_map = {
    'Responsive': 'P3354',  # positive therapeutic predictor
    'Resistant': 'P3355'  # negative therapeutic predictor
}

evidence_level_map = {
    'Pre-clinical': 'Q38145925',
    'Early trials': 'Q38145727',
    'Case report': 'Q38145865',
    'Late trials': 'Q38145539',
    # 'Clinical trial': '',  # ?
    'FDA guidelines': 'Q38145055',  # CGI Evidence Clinical Practice
    'FDA': 'Q38145055',
    'NCCN guidelines': 'Q38145055',
    'European LeukemiaNet guidelines': 'Q38145055',
    'CPIC guidelines': 'Q38145055',
    'NCCN/CAP guidelines': 'Q38145055'
}

source_map = {
    'FDA': 'Q204711',
    'EMA': 'Q130146',
    'NCCN': 'Q6971741'
}
__metadata__ = {'name': 'CGI_Variant_Bot', 'tags': ['variant']}

hgnc_qid = {k.upper(): v for k, v in id_mapper(PROPS['HGNC gene symbol']).items()}

core_props = get_default_core_props()


def create_missense_variant_item(hgvs, label, login, fast_run=True):
    print(hgvs)
    mv = myvariant.MyVariantInfo()
    vd = mv.getvariant(hgvs)
    chrom = human_chromosome_map[vd['chrom'].upper()]
    if 'hg19' not in vd or 'dbnsfp' not in vd:
        raise ValueError("Metadata not found in MyVariant, unable to create item")
    start = str(vd['hg19']['start'])
    end = str(vd['hg19']['end'])
    gene = hgnc_qid[vd['dbnsfp']['genename'].upper()]
    url = "http://myvariant.info/v1/variant/{}".format(quote(hgvs))

    ref = [wdi_core.WDItemID(ITEMS['MyVariant.info'], PROPS['stated in'], is_reference=True),
           wdi_core.WDUrl(url, PROPS['reference URL'], is_reference=True),
           wdi_core.WDTime(strftime("+%Y-%m-%dT00:00:00Z", gmtime()), PROPS['retrieved'], is_reference=True)]
    ga_qual = wdi_core.WDItemID(ITEMS['Genome assembly GRCh37'], PROPS['genomic assembly'], is_qualifier=True)

    s = []
    s.append(wdi_core.WDItemID(ITEMS['sequence variant'], PROPS['instance of'], references=[ref]))
    s.append(wdi_core.WDItemID(ITEMS['Missense Variant'], PROPS['subclass of'], references=[ref]))
    s.append(wdi_core.WDItemID(chrom, PROPS['chromosome'], references=[ref], qualifiers=[ga_qual]))
    s.append(wdi_core.WDString(start, PROPS['genomic start'], references=[ref], qualifiers=[ga_qual]))
    s.append(wdi_core.WDString(end, PROPS['genomic end'], references=[ref], qualifiers=[ga_qual]))
    s.append(wdi_core.WDItemID(gene, PROPS['biological variant of'], references=[ref]))
    s.append(wdi_core.WDExternalID(hgvs, PROPS['HGVS nomenclature'], references=[ref]))

    item = wdi_core.WDItemEngine(data=s, fast_run=fast_run,
                                 fast_run_base_filter={PROPS['HGVS nomenclature']: ''}, fast_run_use_refs=True,
                                 ref_handler=update_retrieved_if_new_multiple_refs, core_props=core_props)
    item.set_label(label)
    item.set_description("genetic variant")
    wdi_helpers.try_write(item, hgvs, PROPS['HGVS nomenclature'], login)
    return item


def create_variant_annotation(variant_qid, association, drug_qid, prim_tt_qid, source, evidence_level, login):
    """

    :param variant_qid: qid of variant
    :param association: {'Responsive', 'Resistant'}
    :param source: semicolon separated str (e.g. "PMID:18552176;PMID:22238366;PMID:23002168", or "FDA")
    :param evidence_level: {'FDA guidelines', 'European LeukemiaNet guidelines', 'NCCN guidelines', 'CPIC guidelines',
                         'NCCN/CAP guidelines'}
    :return:
    """
    print(variant_qid, association, drug_qid, prim_tt_qid, source, evidence_level)

    def create_reference(source_str, evidence_level, login):
        """
        Reference is:
        curator: Cancer Biomarkers database
        retrieved: date
        stated in: links to pmid items
        no reference URL
        """
        reference = [wdi_core.WDItemID(ITEMS['Cancer Biomarkers database'], PROPS['curator'], is_reference=True)]
        t = strftime("+%Y-%m-%dT00:00:00Z", gmtime())
        reference.append(wdi_core.WDTime(t, prop_nr=PROPS['retrieved'], is_reference=True))
        for source in source_str.split(";"):
            if source.startswith("PMID:"):
                qid, _, success = wdi_helpers.PublicationHelper(source.replace("PMID:", ""), id_type="pmid",
                                                                source="europepmc").get_or_create(login)
                if success:
                    reference.append(wdi_core.WDItemID(qid, PROPS['stated in'], is_reference=True))
            elif source in source_map:
                reference.append(wdi_core.WDItemID(source_map[source], PROPS['stated in'], is_reference=True))
            else:
                print("unknown source: {}".format(source))
        return reference

    """
    **Qualifiers**
    medical condition treated: disease
    determination method: one of CGI evidences (in evidence_level_map) (e.g. CGI Evidence Clinical Practice)
    """
    qualifiers = [wdi_core.WDItemID(prim_tt_qid, PROPS['medical condition treated'], is_qualifier=True),
                  wdi_core.WDItemID(evidence_level_map[evidence_level], PROPS['determination method'],
                                    is_qualifier=True)]

    s = wdi_core.WDItemID(drug_qid, association,
                          qualifiers=qualifiers,
                          references=[create_reference(source, evidence_level, login)])
    item = wdi_core.WDItemEngine(data=[s], wd_item_id=variant_qid,
                                 append_value=list(association_map.values()),
                                 fast_run=False, fast_run_use_refs=True,
                                 fast_run_base_filter={PROPS['HGVS nomenclature']: ''}, global_ref_mode='CUSTOM',
                                 ref_handler=update_retrieved_if_new_multiple_refs, core_props=core_props)

    wdi_helpers.try_write(item, variant_qid, '', login)

    item = wdi_core.WDItemEngine(wd_item_id=variant_qid)
    item = remove_old_statements(item)
    item.update(item.statements)
    wdi_helpers.try_write(item, variant_qid, '', login)
    return item


def remove_old_statements(item):
    # remove the statements we added before (without determination method)
    for s in item.statements:
        if (s.get_prop_nr() in association_map.values()) and len(s.qualifiers) == 1:
            if len(s.references) == 1:
                if any(r.get_prop_nr() == "P1640" and r.get_value() == 38100115 for r in s.references[0]):
                    print("removing: {}".format(s))
                    setattr(s, "remove", "")
            else:
                s.references = [ref for ref in s.references if
                                not any(r.get_prop_nr() == "P1640" and r.get_value() == 38100115 for r in ref)]
    return item


def main(df, log_dir="./logs", fast_run=False):
    df = filter_df_clinical_missense(df)
    # df = df.head(2)

    login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS)

    # make sure we have all the variant items we need
    hgvs_qid = id_mapper(PROPS['HGVS nomenclature'])
    for _, row in tqdm(df.iterrows(), total=len(df)):
        if row.gDNA not in hgvs_qid:
            continue
            label = "{} ({})".format(row.gDNA, row['individual_mutation'])
            print("creating {}".format(label))
            try:
                item = create_missense_variant_item(row.gDNA, label, login, fast_run=fast_run)
            except Exception as e:
                print(e)
                wdi_core.WDItemEngine.log("ERROR", wdi_helpers.format_msg(row.gDNA, "gDNA", None, str(e), type(e)))
                continue
            hgvs_qid[row.gDNA] = item.wd_item_id

    for _, row in tqdm(df.iterrows(), total=len(df)):
        if row.gDNA not in hgvs_qid:
            wdi_core.WDItemEngine.log("WARNING", wdi_helpers.format_msg(row.gDNA, "gDNA", None,
                                                                        "variant not found: {}".format(row.gDNA),
                                                                        "variant not found"))
            continue
        if row.Association not in association_map:
            wdi_core.WDItemEngine.log("WARNING", wdi_helpers.format_msg(row.gDNA, "gDNA", None,
                                                                        "Association not found: {}".format(
                                                                            row.Association),
                                                                        "association not found"))
            continue
        qid = hgvs_qid[row.gDNA]
        association = association_map[row.Association]
        drug_qid = row.Drug_qid
        prim_tt_qid = row.prim_tt_qid
        source = row.Source
        evidence_level = row['Evidence level']

        item = create_variant_annotation(qid, association, drug_qid, prim_tt_qid, source, evidence_level, login)


def filter_df_clinical_missense(df):
    # Keep those with clinical evidence only
    clinical_evidence = {'FDA guidelines', 'European LeukemiaNet guidelines', 'NCCN guidelines', 'CPIC guidelines',
                         'NCCN/CAP guidelines'}
    df = df[df['Evidence level'].isin(clinical_evidence)]

    # MUT only, with a HGVS ID
    drop = df[df.gDNA.isnull()]
    df = df[df.gDNA.notnull()]
    for _, row in drop.iterrows():
        wdi_core.WDItemEngine.log("WARNING",
                                  wdi_helpers.format_msg(row.Alteration, "alteration", None, '', "no HGVS ID"))

    # get rid of those where we don't know the drug
    drop = df[df.Drug_qid.isnull()]
    df = df[df.Drug_qid.notnull()]
    for _, row in drop.iterrows():
        wdi_core.WDItemEngine.log("WARNING", wdi_helpers.format_msg(row.Alteration, "alteration", None,
                                                                    "unknown drug: {}".format(row.Drug),
                                                                    "unknown drug"))
    # get rid of the multiple drug ("or") items
    drop = df[df.Drug_qid.str.count(";") != 0]
    df = df[df.Drug_qid.str.count(";") == 0]
    for _, row in drop.iterrows():
        wdi_core.WDItemEngine.log("WARNING", wdi_helpers.format_msg(row.Alteration, "alteration", None,
                                                                    "unknown drug: {}".format(row.Drug),
                                                                    "unknown drug"))

    # get rid of those where we don't know the disease
    drop = df[df.prim_tt_qid.isnull()]
    df = df[df.prim_tt_qid.notnull()]
    for _, row in drop.iterrows():
        wdi_core.WDItemEngine.log("WARNING",
                                  wdi_helpers.format_msg(row.Alteration, "alteration", None,
                                                         "unknown disease: {}".format(row['Primary Tumor type']),
                                                         "unknown disease"))

    return df


if __name__ == "__main__":
    """
    Bot to add/update CGI
    """
    parser = argparse.ArgumentParser(description='run bot')
    parser.add_argument('--path', help='path to file')
    parser.add_argument('--url', help='url to file')
    parser.add_argument('--log-dir', help='directory to store logs', type=str)
    parser.add_argument('--fastrun', dest='fastrun', action='store_true')
    parser.add_argument('--no-fastrun', dest='fastrun', action='store_false')
    parser.set_defaults(fastrun=True)

    args = parser.parse_args()
    if (args.path and args.url) or not (args.path or args.url):
        raise ValueError("must give one of --path and --url")
    log_dir = args.log_dir if args.log_dir else "./logs"
    run_id = datetime.now().strftime('%Y%m%d_%H:%M')
    __metadata__['run_id'] = run_id
    fast_run = args.fastrun

    log_name = '{}-{}.log'.format(__metadata__['name'], run_id)
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(__metadata__),
                                        logger_name='cgi')

    path = args.url if args.url else args.path
    df = pd.read_csv(path, sep="\t")
    main(df, log_dir=log_dir, fast_run=fast_run)
