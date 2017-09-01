import argparse
import json
import os
from datetime import datetime
from time import gmtime, strftime
from urllib.parse import quote

import pandas as pd
import requests
from tqdm import tqdm
import myvariant

from wikidataintegrator import wdi_core, wdi_helpers, wdi_login
from wikidataintegrator.wdi_core import WDItemEngine
from wikidataintegrator.wdi_helpers import id_mapper
from scheduled_bots.geneprotein import human_chromosome_map

try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

PROPS = {
    'CIViC variant ID': 'P3329',
    'HGVS nomenclature': 'P3331',
    'stated in': 'P248',
    'retrieved': 'P813',
    'HGNC gene symbol': 'P353',
    'biological variant of': 'P3433',
    'genomic start': 'P644',
    'genomic end': 'P645',
    'chromosome': 'P1057',
    'genomic assembly': 'P659',
    'instance of': 'P31',
    'subclass of': 'P279',
    'reference URL': 'P854',
    'curator': 'P1640',
    'determination method': 'P459',
    'medical condition treated': 'P2175',
    'has part': 'P527'
}

ITEMS = {
    'Cancer Biomarkers database': 'Q38100115',
    'Genome assembly GRCh37': 'Q21067546',
    'sequence variant': 'Q15304597',
    'Missense Variant': 'Q27429979',
    'MyVariant.info': 'Q38104308',
    'CGI Evidence Clinical Practice': 'Q38145055',
    'CGI Evidence Clinical Trials III-IV': 'Q38145539',
    'CGI Evidence Clinical Trials I-II': 'Q38145727',
    'CGI Evidence Case Reports': 'Q38145865',
    'CGI Evidence Pre-Clinical Data': 'Q38145925',
    'combination therapy': 'Q1304270'
}

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
__metadata__ = {'name': 'CGI_Variant_Bot', 'tags': ['variant'], 'properties': list(PROPS.values())}

hgnc_qid = {k.upper(): v for k, v in id_mapper(PROPS['HGNC gene symbol']).items()}

def update_retrieved_if_new_multiple_refs(olditem, newitem, days=180):
    """
    # modifies olditem in place
    # any ref that does not exactly match the new proposed reference (not including retrieved) is kept
    """

    def is_equal_not_retrieved(oldref, newref):
        """
        Return True if the oldref == newref, NOT including any "retrieved" statements
        :param oldref:
        :param newref:
        :return:
        """
        if len(oldref) != len(newref):
            return False
        oldref_minus_retrieved = [x for x in oldref if x.get_prop_nr() != 'P813']
        newref_minus_retrieved = [x for x in newref if x.get_prop_nr() != 'P813']
        if not all(x in oldref_minus_retrieved for x in newref_minus_retrieved):
            return False
        oldref_retrieved = [x for x in oldref if x.get_prop_nr() == 'P813']
        newref_retrieved = [x for x in newref if x.get_prop_nr() == 'P813']
        if (len(newref_retrieved) != len(oldref_retrieved)):
            return False
        return True

    def ref_overwrite(oldref, newref, days):
        """
        If the newref is the same as the oldref except the retrieved date is `days` newer, return True
                                                       the retrieved date is NOT `days` newer, return False
        the refs are different, return True
        """
        if len(oldref) != len(newref):
            return True
        oldref_minus_retrieved = [x for x in oldref if x.get_prop_nr() != 'P813']
        newref_minus_retrieved = [x for x in newref if x.get_prop_nr() != 'P813']
        if not all(x in oldref_minus_retrieved for x in newref_minus_retrieved):
            return True
        oldref_retrieved = [x for x in oldref if x.get_prop_nr() == 'P813']
        newref_retrieved = [x for x in newref if x.get_prop_nr() == 'P813']
        if (len(newref_retrieved) != len(oldref_retrieved)) or not (
                        len(newref_retrieved) == len(oldref_retrieved) == 1):
            return True
        datefmt = '+%Y-%m-%dT%H:%M:%SZ'
        retold = list([datetime.strptime(r.get_value()[0], datefmt) for r in oldref if r.get_prop_nr() == 'P813'])[0]
        retnew = list([datetime.strptime(r.get_value()[0], datefmt) for r in newref if r.get_prop_nr() == 'P813'])[0]
        return (retnew - retold).days >= days

    newrefs = newitem.references
    oldrefs = olditem.references

    found_mate = [False]*len(newrefs)
    for new_n, newref in enumerate(newrefs):
        for old_n, oldref in enumerate(oldrefs):
            if is_equal_not_retrieved(oldref, newref):
                found_mate[new_n] = True
                if ref_overwrite(oldref, newref, days):
                    oldrefs[old_n] = newref
    for f_idx, f in enumerate(found_mate):
        if not f:
            oldrefs.append(newrefs[f_idx])

def create_missense_variant_item(hgvs, label, login, fast_run=True):
    print(hgvs)
    mv = myvariant.MyVariantInfo()
    vd = mv.getvariant(hgvs)
    chrom = human_chromosome_map[vd['chrom'].upper()]
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

    item = wdi_core.WDItemEngine(item_name=label, data=s, domain="variant", fast_run=fast_run,
                                 fast_run_base_filter={PROPS['HGVS nomenclature']: ''}, fast_run_use_refs=True,
                                 ref_handler=update_retrieved_if_new_multiple_refs)
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
        reference = [
            wdi_core.WDItemID(value=ITEMS['Cancer Biomarkers database'], prop_nr=PROPS['curator'], is_reference=True)]
        t = strftime("+%Y-%m-%dT00:00:00Z", gmtime())
        reference.append(wdi_core.WDTime(t, prop_nr=PROPS['retrieved'], is_reference=True))
        reference.append(
            wdi_core.WDItemID(evidence_level_map[evidence_level], PROPS['determination method'], is_reference=True))
        for source in source_str.split(";"):
            if source.startswith("PMID:"):
                qid = wdi_helpers.PubmedItem(source.replace("PMID:", "")).get_or_create(login)
                reference.append(wdi_core.WDItemID(qid, PROPS['stated in'], is_reference=True))
            elif source in source_map:
                reference.append(wdi_core.WDItemID(source_map[source], PROPS['stated in'], is_reference=True))
            else:
                print("unknown source: {}".format(source))
        return reference

    s = wdi_core.WDItemID(drug_qid, association,
                          qualifiers=[wdi_core.WDItemID(prim_tt_qid, PROPS['medical condition treated'], is_qualifier=True)],
                          references=[create_reference(source, evidence_level, login)])
    item = wdi_core.WDItemEngine(data=[s], wd_item_id=variant_qid, domain='variant',
                                 append_value=list(association_map.values()),
                                 fast_run=True, fast_run_use_refs=True,
                                 fast_run_base_filter={PROPS['HGVS nomenclature']: ''}, global_ref_mode='CUSTOM',
                                 ref_handler=update_retrieved_if_new_multiple_refs)
    wdi_helpers.try_write(item, variant_qid, '', login)
    return item


def main(df, log_dir="./logs", fast_run=False):
    df = filter_df_clinical_missense(df)

    login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS)
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, logger_name='WD_logger', log_name=log_name,
                                        header=json.dumps(__metadata__))

    # make sure we have all the variant items we need
    hgvs_qid = id_mapper(PROPS['HGVS nomenclature'])
    for _, row in tqdm(df.iterrows(), total=len(df)):
        if row.gDNA not in hgvs_qid:
            label = "{} ({})".format(row.gDNA, row['individual_mutation'])
            print("creating {}".format(label))
            try:
                item = create_missense_variant_item(row.gDNA, label, login, fast_run=fast_run)
            except Exception as e:
                print(e)
                wdi_core.WDItemEngine.log("ERROR", wdi_helpers.format_msg(row.gDNA, None, None, "Failed creating variant item: {}".format(e)))
                continue
            hgvs_qid[row.gDNA] = item.wd_item_id

    for _,row in tqdm(df.iterrows(), total=len(df)):
        if row.gDNA not in hgvs_qid:
            wdi_core.WDItemEngine.log("ERROR", wdi_helpers.format_msg(row.gDNA, None, None, "variant not found: {}".format(row.gDNA)))
            continue
        if row.Association not in association_map:
            wdi_core.WDItemEngine.log("ERROR", wdi_helpers.format_msg(row.gDNA, None, None,
                                                                      "Association not found: {}".format(row.Association)))
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
    for _,row in drop.iterrows():
        wdi_core.WDItemEngine.log("WARNING", wdi_helpers.format_msg(row.Alteration, None, None, "no HGVS ID"))

    # get rid of those where we don't know the drug
    drop = df[df.Drug_qid.isnull()]
    df = df[df.Drug_qid.notnull()]
    for _,row in drop.iterrows():
        wdi_core.WDItemEngine.log("WARNING", wdi_helpers.format_msg(row.Alteration, None, None,
                                                                    "unknown drug: {}".format(row.Drug)))
    # get rid of the multiple drug ("or") items
    drop = df[df.Drug_qid.str.count(";")!=0]
    df = df[df.Drug_qid.str.count(";") == 0]
    for _, row in drop.iterrows():
        wdi_core.WDItemEngine.log("WARNING", wdi_helpers.format_msg(row.Alteration, None, None,
                                                                    "unknown drug: {}".format(row.Drug)))

    # get rid of those where we don't know the disease
    drop = df[df.prim_tt_qid.isnull()]
    df = df[df.prim_tt_qid.notnull()]
    for _, row in drop.iterrows():
        wdi_core.WDItemEngine.log("WARNING", wdi_helpers.format_msg(row.Alteration, None, None,
                                                                    "unknown disease: {}".format(row['Primary Tumor type'])))

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
