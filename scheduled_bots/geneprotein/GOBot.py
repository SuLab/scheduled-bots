"""
example microbial protein:
https://www.wikidata.org/wiki/Q22291171
example yeast protein:
https://www.wikidata.org/wiki/Q27553062

Data source: Quickgo mongo

"""
import argparse
import json
import os
import traceback
from collections import defaultdict
from datetime import datetime
from functools import partial
from typing import Dict, Set

import pandas as pd
from scheduled_bots.geneprotein import go_props, go_evidence_codes, curators_wdids, curator_ref, PROPS
from scheduled_bots.utils import get_values
from tqdm import tqdm
from wikidataintegrator import ref_handlers
from wikidataintegrator import wdi_login, wdi_core, wdi_helpers

DAYS = 6 * 30
update_retrieved_if_new = partial(ref_handlers.update_retrieved_if_new_multiple_refs, days=DAYS)

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


def make_go_ref(curator, pmid_map, external_id, uniprot_id, evidence_wdid, retrieved, pmid=None):
    # initialize this reference for this evidence code with retrieved
    reference = [wdi_core.WDTime(retrieved.strftime('+%Y-%m-%dT00:00:00Z'), prop_nr='P813', is_reference=True)]

    # stated in pmid
    if pmid:
        if pmid in pmid_map:
            reference.append(wdi_core.WDItemID(pmid_map[pmid], 'P248', is_reference=True))
        else:
            raise ValueError("article item for pmid {} not found. skipping item".format(pmid))

    # stated in uniprot-GOA Q28018111
    reference.append(wdi_core.WDItemID('Q28018111', 'P248', is_reference=True))

    # curator
    if curator in curators_wdids:
        reference.append(wdi_core.WDItemID(curators_wdids[curator], 'P1640', is_reference=True))
        # curator-specific reference URLs
        # If curator is SGD, add external ID to ref
        if curator in curator_ref and curator_ref[curator] in external_id:
            reference.append(
                wdi_core.WDString(external_id[curator_ref[curator]], PROPS[curator_ref[curator]],
                                  is_reference=True))
    else:
        raise ValueError("curator not found: {}".format(curator))

    # reference URL
    # ref_url = "http://www.ebi.ac.uk/QuickGO/GAnnotation?protein={}".format(uniprot_id)
    ref_url = "http://www.ebi.ac.uk/QuickGO/annotations?protein={}&geneProductId=UniProtKB:{}".format(uniprot_id,
                                                                                                      uniprot_id)
    reference.append(wdi_core.WDString(ref_url, 'P854', is_reference=True))

    # ref determination method
    reference.append(wdi_core.WDItemID(evidence_wdid, 'P459', is_reference=True))

    return reference


def make_go_statements(uniprot_id: str, this_go: pd.DataFrame, go_map: dict, pmid_map: dict, external_id: dict, retrieved):
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
    :param external_id:
    :param login: wdi_core login instance
    :return:
    :
    :type item_wdid:
    """
    statements = []
    for go_id, sub_df in this_go.groupby(level=0):
        aspect = set(sub_df.index.get_level_values("aspect"))
        assert len(aspect) == 1
        aspect = list(aspect)[0]
        level_wdid = go_props[aspect]
        go_wdid = go_map[go_id]

        statement = wdi_core.WDItemID(go_wdid, level_wdid)
        for evidence, ss_df in sub_df.groupby(level=1):
            evidence_wdid = go_evidence_codes[evidence]
            statement.qualifiers.append(wdi_core.WDItemID(value=evidence_wdid, prop_nr='P459', is_qualifier=True))

            # ss_df looks like this:
            """
            go_id       evidence  source  db         aspect
            GO:0000171  IDA       MGI     UniProtKB  Function    [PMID:11413139, PMID:1234]
                                  SGD     UniProtKB  Function     [PMID:9620854]
            """
            # or
            """
            go_id       evidence  source    db         aspect
            GO:0004526  IEA       InterPro  UniProtKB  Function    [GO_REF:0000002]
                                  UniProt   UniProtKB  Function    [GO_REF:0000003]
            """

            for keys, refs in ss_df.items():
                # keys looks like: ('GO:0000171', 'IDA', 'MGI', 'UniProtKB', 'Function')
                # refs looks like: ['PMID:11413139']
                curator = keys[2]
                pmids = set([x[5:] for x in refs if x.startswith("PMID:")])
                if pmids:
                    for pmid in pmids:
                        reference = make_go_ref(curator, pmid_map, external_id, uniprot_id, evidence_wdid, retrieved, pmid=pmid)
                        statement.references.append(reference)
                else:
                    reference = make_go_ref(curator, pmid_map, external_id, uniprot_id, evidence_wdid, retrieved)
                    statement.references.append(reference)

        # not required, but for asthetics...
        statement.references = statement.references[::-1]
        statements.append(statement)

    return statements


def create_articles(pmids: Set[str], login: object, write: bool = True) -> Dict[str, str]:
    """
    Given a list of pmids, make article items for each
    :param pmids: list of pmids
    :param login: wdi_core login instance
    :param write: actually perform write
    :return: map pmid -> wdid
    """
    pmid_map = dict()
    for pmid in pmids:
        p = wdi_helpers.PubmedItem(pmid)
        if write:
            try:
                pmid_wdid = p.get_or_create(login)
            except Exception as e:
                print("Error creating article pmid: {}, error: {}".format(pmid, e))
                continue
            pmid_map[pmid] = pmid_wdid
        else:
            pmid_map[pmid] = 'Q1'
    return pmid_map


def main(taxon, file, retrieved, log_dir="./logs", fast_run=True, write=True):
    """
    Main function for annotating GO terms on proteins
    
    :param taxon: taxon to use (ncbi tax id)
    :type taxid: str
    :param file: path to gaf_file to use. See below for format
    :type str
    :param retrieved: date that the GO annotations were retrieved
    :type retrieved: datetime
    :param log_dir: dir to store logs
    :type log_dir: str
    :param fast_run: use fast run mode
    :type fast_run: bool
    :param write: actually perform write
    :type write: bool
    :return:

    The following columns are expected in the gaf file
    ['db','id','go_id','reference','evidence','aspect','taxon','source']

    This can be created by selecting columns $1,$2,$5,$6,$7,$9,$13,$15 from goa_uniprot_all.gaf.gz
    downloaded from ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/UNIPROT/goa_uniprot_all.gaf.gz

    """
    login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS)
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(__metadata__))

    organism_wdid = wdi_helpers.prop2qid("P685", taxon)
    if not organism_wdid:
        raise ValueError("organism {} not found".format(taxon))
    print("Running organism: {} {}".format(taxon, organism_wdid))

    # get all uniprot id -> wdid mappings, where found in taxon is this organism
    prot_wdid_mapping = wdi_helpers.id_mapper(UNIPROT, (("P703", organism_wdid),))

    # get all goID to wdid mappings
    go_map = wdi_helpers.id_mapper("P686")

    # Get GO terms from our local store for this taxon
    colnames = ['db', 'id', 'go_id', 'reference', 'evidence', 'aspect', 'taxon', 'source']
    df = pd.read_csv(file, sep=' ', names=colnames, index_col=False)
    df = df[df.taxon == "taxon:" + str(taxon)]
    if len(df) == 0:
        print("No GO annotations found for taxid: {}".format(taxon))
        return None
    else:
        print("Found {} GO annotations".format(len(df)))

    # get all pmids and make items for them
    pmids = set([x[5:] for x in df['reference'] if x.startswith("PMID:")])
    print("Need {} pmids".format(len(pmids)))
    pmid_map = get_values("P698", pmids)
    print("Found {} pmids".format(len(pmid_map)))
    pmids_todo = pmids - set(pmid_map.keys())
    print("Creating {} pmid items".format(len(pmids_todo)))
    new_pmids = create_articles(pmids_todo, login, write)
    pmid_map.update(new_pmids)
    print("Done creating pmid items")

    # get all external IDs we may need by uniprot id
    external_ids = defaultdict(dict)
    external_ids_info = {'Saccharomyces Genome Database ID': 'P3406', 'Mouse Genome Informatics ID': 'P671',
                         'UniProt ID': 'P352'}
    for external_id_name, prop in external_ids_info.items():
        id_map = wdi_helpers.id_mapper(prop, (("P703", organism_wdid),))
        if not id_map:
            continue
        for id, wdid in id_map.items():
            external_ids[wdid][external_id_name] = id

    # groupby ID, GOID & evidence, the make references a list
    go_annotations = df.groupby(['id', 'go_id', 'evidence', 'source', 'db', 'aspect'])['reference'].apply(list)

    # iterate through all proteins & write
    failed_items = []
    for uniprot_id, item_wdid in tqdm(prot_wdid_mapping.items()):
        # if uniprot_id != "Q9RJK2":
        #    continue
        if uniprot_id not in go_annotations:
            continue
        this_go = go_annotations[uniprot_id]
        external_id = external_ids[item_wdid]
        # print(this_go)
        try:
            statements = make_go_statements(uniprot_id, this_go, go_map, pmid_map, external_id, retrieved)
            wditem = wdi_core.WDItemEngine(wd_item_id=item_wdid, domain='protein', data=statements, fast_run=fast_run,
                                           fast_run_base_filter={UNIPROT: "", "P703": organism_wdid},
                                           fast_run_use_refs=True,
                                           ref_handler=update_retrieved_if_new,
                                           append_value=['P680', 'P681', 'P682']
                                           )
            wdi_helpers.try_write(wditem, record_id=uniprot_id, record_prop=UNIPROT, edit_summary="update GO terms",
                                  login=login, write=write)
        except Exception as e:
            print(e)
            traceback.print_exc()
            failed_items.append(uniprot_id)
            wdi_core.WDItemEngine.log("ERROR",
                                      wdi_helpers.format_msg(uniprot_id, UNIPROT, item_wdid, str(e),
                                                             msg_type=type(e)))

    print("{} items failed: {}".format(len(failed_items), failed_items))
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []


def get_all_taxa():
    """
    Get all taxa in wikidata with a protein
    :return:
    """
    query = """SELECT ?taxid (COUNT(?protein) AS ?count) WHERE {
                  ?protein wdt:P352 ?uni .
                  ?protein wdt:P703 ?taxa .
                  ?taxa wdt:P685 ?taxid .
                } group by ?taxid order by ?count
                """
    response = wdi_core.WDItemEngine.execute_sparql_query(query=query)
    taxids = [x['taxid']['value'] for x in response['results']['bindings'] if int(x['count']['value']) >= 10]
    return ",".join(sorted(taxids))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='run wikidata GO bot')
    parser.add_argument("file", help="path to gaf file", type=str)
    parser.add_argument('--log-dir', help='directory to store logs', type=str)
    parser.add_argument('--dummy', help='do not actually do write', action='store_true')
    parser.add_argument('--taxon', help="only run using this taxon (required)", type=str)
    parser.add_argument('--retrieved', help="date go annotations were retrieved (YYYYMMDD)", type=str)
    parser.add_argument('--fastrun', dest='fastrun', action='store_true')
    parser.add_argument('--no-fastrun', dest='fastrun', action='store_false')
    parser.set_defaults(fastrun=True)
    args = parser.parse_args()
    log_dir = args.log_dir if args.log_dir else "./logs"
    run_id = datetime.now().strftime('%Y%m%d_%H:%M')
    __metadata__['run_id'] = run_id
    taxon = args.taxon
    file = args.file
    if not taxon:
        raise ValueError("taxon is required")
    fast_run = args.fastrun
    retrieved = datetime.strptime(args.retrieved, "%Y%m%d") if args.retrieved else datetime.now()

    log_name = '{}-{}.log'.format(__metadata__['name'], run_id)
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(__metadata__),
                                        logger_name='go{}'.format(taxon))

    main(taxon, file, retrieved, log_dir=log_dir, fast_run=fast_run, write=not args.dummy)