"""
Gets all gene item in wikidata, where a gene item is an item with an entrez ID, filtering those with no sitelinks
and no items linking to them

Gets all genes in mygene (from the latest mongo dump)

Gets those wd genes that are no longer in mygene, and the proteins they encode (if exists)

Propose for deletion on: https://www.wikidata.org/wiki/Wikidata:Requests_for_deletions

"""
import argparse
import os
from collections import Counter
import itertools

import requests

try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

from scheduled_bots.geneprotein.GeneBot import wdi_core
from pymongo import MongoClient
from scheduled_bots.utils import login_to_wikidata


# todo: add microbial (checked may 2017 there were only 9 deprecated microbial)
# from scheduled_bots.geneprotein.MicrobeBotResources import get_ref_microbe_taxids
# df = get_ref_microbe_taxids()
# ref_taxids = list(map(str, df['taxid'].tolist()))


def get_deprecated_genes(taxids=None):
    if taxids is None:
        taxids = ['3702', '559292', '123', '10090', '9606', '10116', '243161', '10116', '7227', '6239', '7955',
                  '515635', '765698', '525903', '759362', '565050', '446465', '9545', '9913']
    taxid_str = '{' + " ".join(['"' + x + '"' for x in taxids]) + '}'

    # get all genes that DONT Have any sitelinks and dont have any item links to them
    s = """SELECT DISTINCT ?entrez ?item ?prot WHERE
    {
      values ?taxids {taxid}
      ?taxon wdt:P685 ?taxids .
      ?item wdt:P351 ?entrez .
      ?item wdt:P703 ?taxon .
      FILTER NOT EXISTS {?article schema:about ?item}
      OPTIONAL {?item wdt:P688 ?prot}
      FILTER NOT EXISTS {?something ?prop ?item }
    }""".replace("{taxid}", taxid_str)
    bindings = wdi_core.WDItemEngine.execute_sparql_query(s)['results']['bindings']
    entrez_qid = {x['entrez']['value']: x['item']['value'].rsplit("/")[-1] for x in bindings}
    gene_protein = {x['item']['value'].rsplit("/")[-1]: x['prot']['value'].rsplit("/")[-1] for x in bindings if
                    'prot' in x}

    print("{} wikidata".format(len(entrez_qid)))
    wd = set(entrez_qid.keys())
    coll = MongoClient().wikidata_src.mygene
    mygene = set([str(x['entrezgene']) for x in coll.find({'entrezgene': {'$exists': True}})])
    print("{} mygene".format(len(mygene)))
    missing = wd - mygene
    print("{} deprecated".format(len(missing)))
    qids = {entrez_qid[x] for x in missing}
    # dont delete the protein items because often there is a new gene (that replaced this deprecated gene,
    # that now encodes this protein. We should just check them, there are currently only 9 out of
    # a thousand something deprecated genes
    protein_qids = {gene_protein[x] for x in qids if x in gene_protein}
    print("Check these protein items: {}".format(protein_qids))
    # qids.update(protein_qids)
    return qids


def grouper(n, iterable):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


def make_deletion_templates(qids, title, reason):
    s = '\n=={}==\n'.format(title)
    for group in grouper(90, list(qids)):
        del_template = "{{subst:Rfd group | {q} | reason = {reason} }}\n".replace("{q}", '|'.join(group)).replace(
            "{reason}", reason)
        s += del_template
    return s


def create_rfd(s: str):
    edit_token, edit_cookie = login_to_wikidata(WDUSER, WDPASS)
    data = {'action': 'edit', 'title': 'Wikidata:Requests_for_deletions',
            'appendtext': s, 'format': 'json', 'token': edit_token}
    r = requests.post("https://www.wikidata.org/w/api.php", data=data, cookies=edit_cookie)
    r.raise_for_status()
    print(r.json())


def get_count_by_species(missing):
    # Get counts of deprecated genes by species
    # not required, just for fun
    s = """Select ?entrez ?taxid where {
    ?item wdt:P351 ?entrez .
    ?item wdt:P703 ?taxon .
    ?taxon wdt:P685 ?taxid .
    }
    """
    entrez_taxid = {x['entrez']['value']: x['taxid']['value'] for x in
                    wdi_core.WDItemEngine.execute_sparql_query(s)['results']['bindings']}
    return Counter([entrez_taxid[x] for x in missing])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='run wikidata gene bot')
    parser.add_argument('--title', help='deletion request title', type=str, default="Delete deprecated genes")
    parser.add_argument('--reason', help='deletion request reason', type=str,
                        default="These genes are deprecated by NCBI")
    parser.add_argument('--force', help='force run if deleting a large number of genes', action='store_true')
    args = parser.parse_args()

    qids = get_deprecated_genes()
    if len(qids) > 200 and not args.force:
        raise ValueError("Trying to delete {} genes. If you really want to do this, re run with --force".format(len(qids)))
    if len(qids) > 0:
        s = make_deletion_templates(qids, args.title, args.reason)
        create_rfd(s)
        log_path = "deletion_log.txt"
        with open(log_path, 'w') as f:
            f.write("\n".join(qids))

