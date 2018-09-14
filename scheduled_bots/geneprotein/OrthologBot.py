"""
https://bitbucket.org/sulab/wikidatabots/src/4f2e4bdf3d7328eb6fd94cc67af61e194bda0a96/genes/orthologs/human/parseHomologene.py?at=dronetest_DiseaseBot&fileviewer=file-view-default

https://www.wikidata.org/wiki/Q14911732#P684

https://www.wikidata.org/wiki/Q18049645

homologene release 68
https://www.wikidata.org/wiki/Q20976936

"""

import argparse
import json
import os
from collections import defaultdict
from datetime import datetime

from tqdm import tqdm

from scheduled_bots import get_default_core_props, PROPS
from scheduled_bots.geneprotein import HelperBot
from scheduled_bots.geneprotein.Downloader import MyGeneDownloader
from wikidataintegrator import wdi_login, wdi_core, wdi_helpers

core_props = get_default_core_props()

try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

__metadata__ = {'name': 'OrthologBot',
                'maintainer': 'GSS',
                'tags': ['gene', 'ortholog'],
                }


def main(metadata, log_dir="./logs", fast_run=True, write=True):
    """
    Main function for creating/updating genes

    :param metadata: looks like: {"ensembl" : 84, "cpdb" : 31, "netaffy" : "na35", "ucsc" : "20160620", .. }
    :type metadata: dict
    :param log_dir: dir to store logs
    :type log_dir: str
    :param fast_run: use fast run mode
    :type fast_run: bool
    :param write: actually perform write
    :type write: bool
    :return: None
    """

    # login
    login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS)
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, logger_name='WD_logger', log_name=log_name,
                                        header=json.dumps(__metadata__))

    # get all ids mappings
    entrez_wdid = wdi_helpers.id_mapper(PROPS['Entrez Gene ID'])
    wdid_entrez = {v: k for k, v in entrez_wdid.items()}
    homo_wdid = wdi_helpers.id_mapper(PROPS['HomoloGene ID'], return_as_set=True)
    wdid_homo = dict()
    for homo, wdids in homo_wdid.items():
        for wdid in wdids:
            wdid_homo[wdid] = homo
    entrez_homo = {wdid_entrez[wdid]: homo for wdid, homo in wdid_homo.items() if wdid in wdid_entrez}
    taxon_wdid = wdi_helpers.id_mapper(PROPS['NCBI Taxonomy ID'])

    # only do certain records
    mgd = MyGeneDownloader(q="_exists_:homologene AND type_of_gene:protein-coding",
                           fields=','.join(['taxid', 'homologene', 'entrezgene']))
    docs, total = mgd.query()
    docs = list(tqdm(docs, total=total))
    records = HelperBot.tag_mygene_docs(docs, metadata)

    # group together all orthologs
    # d[taxid][entrezgene] = { set of entrezgene ids for orthologs }
    d = defaultdict(lambda: defaultdict(set))
    entrez_taxon = dict()  # keep this for the qualifier on the statements
    for doc in records:
        this_taxid = doc['taxid']['@value']
        this_entrez = doc['entrezgene']['@value']
        entrez_taxon[str(this_entrez)] = str(this_taxid)
        if str(this_entrez) not in entrez_wdid:
            continue
        for taxid, entrez in doc['homologene']['@value']['genes']:
            if taxid == 4932 and this_taxid == 559292:
                # ridiculous workaround because entrez has the taxid for the strain and homologene has it for the species
                # TODO: This needs to be fixed if you want to use other things that may have species/strains .. ?`
                continue
            if taxid != this_taxid and str(entrez) in entrez_wdid:
                d[str(this_taxid)][str(this_entrez)].add(str(entrez))

    print("taxid: # of genes  : {}".format({k: len(v) for k, v in d.items()}))

    homogene_ver = metadata['homologene']
    release = wdi_helpers.Release("HomoloGene build{}".format(homogene_ver), "Version of HomoloGene", homogene_ver,
                                  edition_of_wdid='Q468215',
                                  archive_url='ftp://ftp.ncbi.nih.gov/pub/HomoloGene/build{}/'.format(
                                      homogene_ver)).get_or_create(login)

    reference = lambda homogeneid: [wdi_core.WDItemID(release, PROPS['stated in'], is_reference=True),
                                    wdi_core.WDExternalID(homogeneid, PROPS['HomoloGene ID'], is_reference=True)]

    ec = 0
    for taxid, subd in tqdm(d.items()):
        for entrezgene, orthologs in tqdm(subd.items(), leave=False):
            try:
                do_item(entrezgene, orthologs, reference, entrez_homo, entrez_taxon, taxon_wdid, entrez_wdid, login,
                        write)
            except Exception as e:
                wdi_helpers.format_msg(entrezgene, PROPS['Entrez Gene ID'], None, str(e), type(e))
                ec += 1
        # clear the fast run store once we move on to the next taxon
        wdi_core.WDItemEngine.fast_run_store = []
        wdi_core.WDItemEngine.fast_run_container = None

    print("Completed succesfully with {} exceptions".format(ec))


def do_item(entrezgene, orthologs, reference, entrez_homo, entrez_taxon, taxon_wdid, entrez_wdid, login, write):
    entrezgene = str(entrezgene)
    s = []
    this_ref = reference(entrez_homo[entrezgene])
    for ortholog in orthologs:
        ortholog = str(ortholog)
        if ortholog == entrezgene:
            continue
        if ortholog not in entrez_taxon:
            raise ValueError("missing taxid for: " + ortholog)
        qualifier = wdi_core.WDItemID(taxon_wdid[entrez_taxon[ortholog]], PROPS['found in taxon'], is_qualifier=True)
        s.append(wdi_core.WDItemID(entrez_wdid[ortholog], PROPS['ortholog'],
                                   references=[this_ref], qualifiers=[qualifier]))
    item = wdi_core.WDItemEngine(wd_item_id=entrez_wdid[entrezgene], data=s, fast_run=fast_run,
                                 fast_run_base_filter={PROPS['Entrez Gene ID']: '',
                                                       PROPS['found in taxon']: taxon_wdid[entrez_taxon[entrezgene]]},
                                 core_props=core_props)
    wdi_helpers.try_write(item, entrezgene, PROPS['Entrez Gene ID'], edit_summary="edit orthologs", login=login,
                          write=write)
    # print(item.wd_item_id)


if __name__ == "__main__":
    """
    Data to be used is retrieved from mygene.info
    """
    parser = argparse.ArgumentParser(description='run wikidata gene bot')
    parser.add_argument('--log-dir', help='directory to store logs', type=str)
    parser.add_argument('--dummy', help='do not actually do write', action='store_true')
    parser.add_argument('--fastrun', dest='fastrun', action='store_true')
    parser.add_argument('--no-fastrun', dest='fastrun', action='store_false')
    parser.set_defaults(fastrun=True)
    args = parser.parse_args()
    log_dir = args.log_dir if args.log_dir else "./logs"
    run_id = datetime.now().strftime('%Y%m%d_%H:%M')
    __metadata__['run_id'] = run_id
    fast_run = args.fastrun

    # get metadata about sources
    mgd = MyGeneDownloader()
    metadata = mgd.get_metadata()['src_version']

    log_name = '{}-{}.log'.format(__metadata__['name'], run_id)
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(__metadata__),
                                        logger_name='orthologs')

    main(metadata, log_dir=log_dir, fast_run=fast_run, write=not args.dummy)
