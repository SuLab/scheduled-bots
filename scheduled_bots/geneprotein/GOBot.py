"""
example microbial protein:
https://www.wikidata.org/wiki/Q22291171
example yeast protein:
https://www.wikidata.org/wiki/Q27553062

"""
import argparse
import json
import os
from itertools import chain

from datetime import datetime
import pandas as pd
import pymongo
from pandas.parser import k
from pymongo import MongoClient
from tqdm import tqdm
from wikidataintegrator import wdi_login, wdi_core, wdi_helpers

from scheduled_bots.geneprotein import go_props, go_evidence_codes, curators_wdids, sources_wdids

try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

__metadata__ = {'name': 'GOBot',
                'maintainer': 'GSS',
                'tags': ['protein', 'GO'],
                'properties': []
                }

ENTREZ = "P351"
UNIPROT = "P352"


def make_go_statements(item_wdid: str, uniprot_id: str, this_go: pd.DataFrame, retrieved: datetime, go_map: dict, pmid_map: dict, login: wdi_login.WDLogin):
    """
    add go terms to a protein item.
    Follows the schema described here:
    https://www.wikidata.org/w/index.php?title=User:ProteinBoxBot/evidence&oldid=410152984#Guidelines_for_referencing_Gene_Ontology_annotations_.28and_template_for_other_complex_annotations_coming_from_multiple_parties_and_presented_through_multiple_aggregation_services.29

    :param item_wdid: The protein item's wikidata item that will have GO terms added/updated
    :param uniprot_id: protein item's uniprot id
    :param this_go: pandas df containing GO annotations for this protein
    :param retrieved: date that the GO annotations were retrieved
    :param go_map: mapping of GO terms from GOID -> wdid
    :param pmid_map: mapping of PMIDs to wdid
    :param login: wdi_core login instance
    :return:
    :
    :type item_wdid:
    """
    statements = []
    for go_id, sub_df in this_go.groupby(level=0):
        level = set(sub_df.index.get_level_values("Aspect"))
        assert len(level) == 1
        level_wdid = go_props[list(level)[0]]
        go_wdid = go_map[go_id]

        statement = wdi_core.WDItemID(go_wdid, level_wdid)
        reference = []
        for evidence, ss_df in sub_df.groupby(level=1):
            evidence_wdid = go_evidence_codes[evidence]
            statement.qualifiers.append(wdi_core.WDItemID(value=evidence_wdid, prop_nr='P459', is_qualifier=True))

            # initialize this reference for this evidence code with retrieved
            reference = [wdi_core.WDTime(retrieved.strftime('+%Y-%m-%dT00:00:00Z'), prop_nr='P813', is_reference=True)]

            # stated in pmids
            pmids = set([x[5:] for x in list(chain(*list(ss_df))) if x.startswith("PMID:")])
            for pmid in pmids:
                reference.append(wdi_core.WDItemID(pmid_map[pmid], 'P248', is_reference=True))

            # stated in dbs
            dbs = set(ss_df.index.get_level_values("DB"))
            for db in dbs:
                reference.append(wdi_core.WDItemID(sources_wdids[db], 'P248', is_reference=True))

            # curators
            curators = list(ss_df.index.get_level_values("Source"))
            for curator in curators:
                if curator in curators_wdids:
                    reference.append(wdi_core.WDItemID(curators_wdids[curator], 'P1640', is_reference=True))
                else:
                    wdi_core.WDItemEngine.logger.warning(
                        wdi_helpers.format_msg(uniprot_id, UNIPROT, item_wdid, "curator not found: {}".format(curator)))
                    print("curator not found: {}".format(curator))

            # reference URL
            ref_url = "http://www.ebi.ac.uk/QuickGO/GAnnotation?protein={}".format(uniprot_id)
            reference.append(wdi_core.WDString(ref_url, 'P854', is_reference=True))

            # ref determination method
            reference.append(wdi_core.WDItemID(evidence_wdid, 'P459', is_reference=True))

            statement.references.append(reference)

        statements.append(statement)

    return statements


def main(coll: pymongo.collection.Collection, taxon: str, retrieved: datetime, log_dir: str = "./logs", write: bool = True) -> None:
    """
    Main function for annotating GO terms on proteins
    
    :param coll: mongo collection containing GO annotations
    :param taxon: taxon to use (ncbi tax id)
    :param retrieved: date that the GO annotations were retrieved
    :param log_dir: dir to store logs
    :param write: actually perform write
    :return: 
    """
    login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS)
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(__metadata__))

    organism_wdid = wdi_helpers.prop2qid("P685", taxon)
    if not organism_wdid:
        raise ValueError("organism {} not found".format(taxon))

    # get all uniprot id -> wdid mappings, where found in taxon is this organism
    prot_wdid_mapping = wdi_helpers.id_mapper(UNIPROT, (("P703", organism_wdid),))

    # get all goID to wdid mappings
    go_map = wdi_helpers.id_mapper("P686")

    # Get GO terms from our local store for this taxon
    df = pd.DataFrame(list(coll.find({'Taxon': int(taxon)})))

    # get all pmids and make items for them
    pmids = set([x[5:] for x in df['Reference'] if x.startswith("PMID:")])
    #pmid_map = wdi_helpers.id_mapper("P698")
    pmid_map = dict()
    pmids_todo = pmids - set(pmid_map.keys())
    print("Creating {} pmid items".format(len(pmids_todo)))

    for pmid in pmids_todo:
        p = wdi_helpers.PubmedStub(pmid)
        #pmid_map[pmid] = "Q1"
        #continue
        if write:
            pmid_wdid = p.get_or_create(login)
            pmid_map[pmid] = pmid_wdid
        else:
            pmid_map[pmid] = "Q1"
    print("Done creating pmid items")

    # groupby ID, GOID & evidence, the make references a list
    go_annotations = df.groupby(['ID', 'GO ID', 'Evidence', 'Source', 'DB', 'Aspect'])['Reference'].apply(list)

    # iterate through all proteins & write
    failed_items=[]
    for uniprot_id, item_wdid in tqdm(prot_wdid_mapping.items()):
        if uniprot_id not in go_annotations:
            continue
        if item_wdid != "Q27551834":
            pass
        this_go = go_annotations[uniprot_id]
        statements = make_go_statements(item_wdid, uniprot_id, this_go, retrieved, go_map, pmid_map, login)

        try:
            wditem = wdi_core.WDItemEngine(wd_item_id=item_wdid, domain='protein', data=statements, fast_run=True,
                                           fast_run_base_filter={UNIPROT: "", "P703": organism_wdid})
            wdi_helpers.try_write(wditem, record_id=uniprot_id, record_prop=UNIPROT, edit_summary="update GO terms",
                                  login=login, write=write)
        except Exception as e:
            print(e)
            failed_items.append(uniprot_id)
    print("{} items failed: {}".format(len(failed_items), failed_items))



# taxon = "559292"  # yeast Q27510868

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='run wikidata GO bot')
    parser.add_argument('--log-dir', help='directory to store logs', type=str)
    parser.add_argument('--dummy', help='do not actually do write', action='store_true')
    parser.add_argument('--taxon', help="only run using this taxon", type=str)
    parser.add_argument('--mongo-uri', type=str, default="mongodb://localhost:27017")
    parser.add_argument('--mongo-db', type=str, default="wikidata_src")
    parser.add_argument('--mongo-coll', type=str, default="quickgo")
    parser.add_argument('--retrieved', help="date go annotations were retrieved (YYYYMMDD)", type=str)
    args = parser.parse_args()
    log_dir = args.log_dir if args.log_dir else "./logs"
    run_id = datetime.now().strftime('%Y%m%d_%H:%M')
    taxon = args.taxon
    coll = MongoClient(args.mongo_uri)[args.mongo_db][args.mongo_coll]
    retrieved = datetime.strptime(args.retrieved, "%Y%m%d") if args.retrieved else datetime.now()

    log_name = '{}-{}.log'.format(__metadata__['name'], datetime.now().strftime('%Y%m%d_%H:%M'))
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(__metadata__), logger_name='go{}'.format(taxon))

    main(coll, taxon, retrieved, log_dir=log_dir, write=not args.dummy)
