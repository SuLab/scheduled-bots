"""
example microbial protein:
https://www.wikidata.org/wiki/Q22291171

Do not use ND terms (http://geneontology.org/page/nd-no-biological-data-available)

"""
import argparse
import copy
import json
import os
from datetime import datetime
from itertools import chain

import pandas as pd
from wikidataintegrator import wdi_login, wdi_core, wdi_helpers
from tqdm import tqdm

from . import go_props, go_evidence_codes, try_write, curators_wdids, sources_wdids

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


def make_go_statements(wdid, uniprot_id, this_go, retrieved, go_wdid_mapping, login):
    """
    add go terms to a protein item
    """
    statements = []
    for go_id, sub_df in this_go.groupby(level=0):
        level = set(sub_df.index.get_level_values("Aspect"))
        assert len(level) == 1
        level_wdid = go_props[list(level)[0]]
        go_wdid = go_wdid_mapping[go_id]

        statement = wdi_core.WDItemID(go_wdid, level_wdid)
        reference = []
        for evidence, ss_df in sub_df.groupby(level=1):
            evidence_wdid = go_evidence_codes[evidence]
            statement.qualifiers.append(wdi_core.WDItemID(value=evidence_wdid, prop_nr='P459',is_qualifier=True))

            # initialize this reference for this evidence code with retrieved
            reference = [wdi_core.WDTime(retrieved.strftime('+%Y-%m-%dT00:00:00Z'), prop_nr='P813', is_reference=True)]

            # stated in pmids
            pmids = [x[5:] for x in list(chain(*list(ss_df))) if x.startswith("PMID:")]
            for pmid in pmids:
                pmid_wdid = wdi_helpers.PubmedStub(pmid).create(login)
                #pmid_wdid = 'Q1'
                reference.append(wdi_core.WDItemID(pmid_wdid, 'P248', is_reference=True))

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
                    print("not found: {}".format(curator))

            # reference URL
            reference.append(wdi_core.WDString("http://www.ebi.ac.uk/QuickGO/GAnnotation?protein={}".format(uniprot_id), 'P854', is_reference=True))

            # ref det method
            reference.append(wdi_core.WDItemID(evidence_wdid, 'P459', is_reference=True))

            statement.references.append(reference)

        statements.append(statement)

    return statements

# relate database to the external ID prop that should be used for the ref
source_ref_id = {'ensembl': 'ensembl_protein',
                 'entrez': 'entrez_gene',
                 'swiss_prot': 'uniprot'}



def main(coll, taxon, retrieved, log_dir="./logs"):

    login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS)
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(__metadata__))

    organism_wdid = wdi_helpers.prop2qid("P685", taxon)

    # get all uniprot id -> wdid mappings, where found in taxon is this strain
    prot_wdid_mapping = wdi_helpers.id_mapper(UNIPROT, (("P703", organism_wdid),))

    # get all goID to wdid mappings
    go_wdid_mapping = wdi_helpers.id_mapper("P686")

    ## pre process GO terms
    df = pd.DataFrame(list(coll.find({'Taxon': taxon})))
    go_annotations = df.groupby(['ID', 'GO ID', 'Evidence', 'Source', 'DB', 'Aspect'])['Reference'].apply(list)

    for uniprot_id, item_wdid in tqdm(prot_wdid_mapping.items()):
        this_go = go_annotations[uniprot_id]
        statements = make_go_statements(item_wdid, uniprot_id, this_go, retrieved, go_wdid_mapping, login)

        wditem = wdi_core.WDItemEngine(wd_item_id=item_wdid, domain='protein', data=statements)
        wditem.write(login)

# taxon = "559292"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='run wikidata GO bot')
    parser.add_argument('--taxon', type=str)
    parser.add_argument('--mongo-uri', type=str)
    parser.add_argument('--interpro-version', type=str)
    parser.add_argument('--interpro-date', type=str)
    args = parser.parse_args()
    log_dir = args.log_dir if args.log_dir else "./logs"
    login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS)

    log_name = 'YeastBot_protein-{}.log'.format(run_id)
    __metadata__['log_name'] = log_name
    __metadata__['sources'] = get_source_versions()

    main(log_dir=log_dir, login=login, )