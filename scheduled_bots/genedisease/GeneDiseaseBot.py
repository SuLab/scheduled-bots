import argparse
import json
import os
from collections import defaultdict
from datetime import datetime
from functools import partial
from time import gmtime, strftime

import pytz
from tqdm import tqdm

from scheduled_bots import PROPS, get_default_core_props
from scheduled_bots.utils import get_values
from wikidataintegrator import ref_handlers
from wikidataintegrator import wdi_core, wdi_helpers, wdi_login
from wikidataintegrator.wdi_helpers import id_mapper

DAYS = 6 * 30
update_retrieved_if_new = partial(ref_handlers.update_retrieved_if_new, days=DAYS)
GWAS_PATH = "GWAS_Catalog.tsv"
core_props = get_default_core_props()

try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

__metadata__ = {
    'name': 'GeneDiseaseBot',
    'tags': ['gene', 'disease'],
}


class GWASCatalog(object):
    def __init__(self, catalog_tsv_path=GWAS_PATH):
        self.data = set()
        self.header = None
        line_num = 0
        with open(catalog_tsv_path, 'r') as f:
            for line in f:
                line = line.strip()

                # Ignore header comments
                if line.startswith("#"):
                    continue

                line_num += 1

                # Parse field headers
                if line_num == 1:
                    self.header = line.split("\t")
                    continue

                # Parse TSV
                try:
                    fields = line.split("\t")
                    for phenotype, uri in zip(fields[4].split(";"), fields[6].split(";")):
                        # Skip non Disease Ontology entries
                        if "DOID" not in uri:
                            continue

                        gdr = GeneDiseaseRelationship(ncbi=fields[1],
                                                      symbol=fields[2],
                                                      taxon=fields[3],
                                                      relationship=fields[5],
                                                      doid=uri.split("_")[1],
                                                      phenotype=phenotype,
                                                      pmid=fields[7],
                                                      link=fields[8])

                        self.data.add(gdr)
                except IndexError:
                    raise ValueError("TSV Parsing failed for line: '{}'".format(line))


class GeneDiseaseRelationship:
    phenocarta_ref_source = 'https://gemma.msl.ubc.ca/phenotypes.html?phenotypeUrlId=DOID_{doid}&ncbiId={ncbi}'

    def __init__(self, ncbi, symbol, taxon, relationship, doid, phenotype, pmid, link):
        self.ncbi = ncbi
        self.symbol = symbol
        self.taxon = taxon
        self.relationship = relationship
        self.doid = doid
        self.phenotype = phenotype
        self.pmid = pmid
        self.link = link

        self.phenocarta_url = GeneDiseaseRelationship.phenocarta_ref_source.format(doid=self.doid, ncbi=self.ncbi)

    def __hash__(self):
        return hash((self.ncbi, self.doid))

    def __eq__(self, other):
        return (self.ncbi, self.doid) == (other.ncbi, other.doid)

    def __ne__(self, other):
        return not (self == other)


class GeneDiseaseBot(object):
    def __init__(self, catalog_tsv_path=GWAS_PATH, login=None, fast_run=False, write=True):
        self.gwas_catalog = GWASCatalog(catalog_tsv_path=catalog_tsv_path)

        self.fast_run_base_gene_filter = {PROPS['Entrez Gene ID']: "", PROPS['found in taxon']: 'Q15978631'}
        self.fast_run_base_disease_filter = {PROPS['Disease Ontology ID']: ""}
        self.login = login
        self.write = write
        self.fast_run = fast_run

        # Load doid -> wdid mapper
        self.doid_wdid_map = id_mapper(PROPS['Disease Ontology ID'])

        # Load entrez gene -> wdid mapper
        self.gene_wdid_map = id_mapper(PROPS['Entrez Gene ID'], filters=[(PROPS['found in taxon'], 'Q15978631')])

        # Load pmid -> wdid mapper
        # No! this eats up like 8gb of RAM!!!!
        # self.pmid_wdid_map = id_mapper(PROPS['PubMed ID'])

        self.wd_items = []

    def run(self):
        wd_genes = defaultdict(list)
        wd_diseases = defaultdict(list)
        gdrs = list(self.gwas_catalog.data)

        print("Get or create references")
        pmids = set([x.pmid for x in gdrs])
        print("Need {} pmids".format(len(pmids)))
        self.pmid_qid_map = get_values("P698", pmids)
        print("Found {} pmids".format(len(self.pmid_qid_map)))
        for pmid in pmids - set(self.pmid_qid_map.keys()):
            qid, _, success = wdi_helpers.PublicationHelper(pmid.replace("PMID:", ""), id_type="pmid",
                                                            source="europepmc").get_or_create(self.login)
            if success:
                self.pmid_qid_map[pmid] = qid

        print("Building relationships & references")
        for gdr in tqdm(gdrs):
            try:
                # Retrieve Wikidata ID for this disease phenotype
                doid_wdid = self.doid_wdid_map["DOID:{}".format(gdr.doid)]
            except KeyError as e:
                msg = "Missing DOID Disease WD Item; skipping {}".format(gdr.doid)
                print(msg)
                wdi_core.WDItemEngine.log("ERROR",
                                          wdi_helpers.format_msg(gdr.doid, PROPS['Disease Ontology ID'], None, msg,
                                                                 type(e)))
                continue

            try:
                # Retrieve Wikidata ID for this gene
                gene_wdid = self.gene_wdid_map[gdr.ncbi]
            except KeyError as e:
                msg = "Missing NCBI Gene WD Item; skipping {}".format(gdr.ncbi)
                print(msg)
                wdi_core.WDItemEngine.log("ERROR",
                                          wdi_helpers.format_msg(gdr.ncbi, PROPS['Entrez Gene ID'], None, msg, type(e)))
                continue

            try:
                items = self.process_relationship(gene_wdid, doid_wdid, gdr)
            except Exception as e:
                print(e)
                wdi_core.WDItemEngine.log("ERROR",
                                          wdi_helpers.format_msg(gdr.ncbi, PROPS['Entrez Gene ID'], None, str(e),
                                                                 type(e)))
                continue

            gdr.gene_wditem = items['gene_item']
            gdr.disease_wditem = items['disease_item']

            # Aggregating data to reduce wikidata updates
            wd_genes[gene_wdid].append(gdr)
            wd_diseases[doid_wdid].append(gdr)

        print("Begin creating Wikidata Gene items with new relationships")

        # Create Wikidata items for genes
        for wdid, gdrs in tqdm(wd_genes.items()):
            # Attach updated disease information to gene
            try:
                gene_wd_item = wdi_core.WDItemEngine(wd_item_id=wdid,
                                                     data=[gdr.disease_wditem for gdr in gdrs],
                                                     domain="genes",
                                                     append_value=[PROPS["genetic association"]],
                                                     fast_run=self.fast_run,
                                                     fast_run_base_filter=self.fast_run_base_gene_filter,
                                                     fast_run_use_refs=True,
                                                     ref_handler=update_retrieved_if_new,
                                                     global_ref_mode="CUSTOM",
                                                     core_props=core_props)
                wd_item = {'item': gene_wd_item, 'record_id': gdrs[0].ncbi, 'record_prop': PROPS['Entrez Gene ID']}
                self.write_item(wd_item)
            except Exception as e:
                msg = "Problem Creating Gene WDItem; skipping {}".format(gdr.ncbi)
                wdi_core.WDItemEngine.log("ERROR",
                                          wdi_helpers.format_msg(gdr.ncbi, PROPS['Entrez Gene ID'], wdid, msg, type(e)))

        print("Begin creating Wikidata Disease items with new relationships")
        for wdid, gdrs in tqdm(wd_diseases.items()):
            # Attach updated gene information to disease
            try:
                disease_wd_item = wdi_core.WDItemEngine(wd_item_id=wdid,
                                                        data=[gdr.gene_wditem for gdr in gdrs],
                                                        domain="diseases",
                                                        append_value=[PROPS["genetic association"]],
                                                        fast_run=self.fast_run,
                                                        fast_run_base_filter=self.fast_run_base_disease_filter,
                                                        fast_run_use_refs=True,
                                                        ref_handler=update_retrieved_if_new,
                                                        global_ref_mode="CUSTOM",
                                                        core_props=core_props)
                wd_item = {'item': disease_wd_item, 'record_id': "DOID:{}".format(gdrs[0].doid),
                           'record_prop': PROPS['Disease Ontology ID']}
                self.write_item(wd_item)
            except Exception as e:
                msg = "Problem Creating Disease WDItem; skipping {}".format(gdr.doid)
                wdi_core.WDItemEngine.log("ERROR",
                                          wdi_helpers.format_msg(gdr.doid, PROPS['Disease Ontology ID'], wdid, msg,
                                                                 type(e)))

    def write_item(self, wd_item):
        wdi_helpers.try_write(wd_item['item'], record_id=wd_item['record_id'],
                              record_prop=wd_item['record_prop'], edit_summary='edit genetic association',
                              login=self.login, write=self.write)

    def process_relationship(self, gene_wdid, doid_wdid, gdr):
        """
        Process will involve creating disease items with references
        to the genes and vice-versa.
        """

        # Create updated references
        genetic_assoc_ref = self.create_references(gdr)
        qualifiers = self.create_qualifiers(gdr)

        # Attach the created genetic association references to this disease phenotype
        disease_item = wdi_core.WDItemID(value=doid_wdid, prop_nr=PROPS["genetic association"],
                                         references=[genetic_assoc_ref], qualifiers=qualifiers,
                                         check_qualifier_equality=False)

        # Repeat for attaching updated gene information to disease

        # Create updated references
        genetic_assoc_ref = self.create_references(gdr)
        qualifiers = self.create_qualifiers(gdr)

        # Attach the created genetic association references to this disease phenotype
        gene_item = wdi_core.WDItemID(value=gene_wdid, prop_nr=PROPS["genetic association"],
                                      references=[genetic_assoc_ref], qualifiers=qualifiers,
                                      check_qualifier_equality=False)

        return {'disease_item': disease_item, 'gene_item': gene_item}

    def create_references(self, gdr):

        references = []

        # Reference URL for phenocarta
        references.append(wdi_core.WDUrl(value=gdr.phenocarta_url, prop_nr=PROPS['reference URL'], is_reference=True))

        # Reference URL for genome.gov
        references.append(wdi_core.WDUrl(value=gdr.link, prop_nr=PROPS['reference URL'], is_reference=True))

        # Stated in Phenocarta
        references.append(wdi_core.WDItemID(value='Q22330995', prop_nr=PROPS['stated in'], is_reference=True))

        # Stated in PubMed
        references.append(wdi_core.WDItemID(value=self.pmid_qid_map[gdr.pmid], prop_nr=PROPS['stated in'],
                                            is_reference=True))

        # Date retrieved
        references.append(
            wdi_core.WDTime(strftime("+%Y-%m-%dT00:00:00Z", gmtime()), prop_nr=PROPS['retrieved'], is_reference=True))

        return references

    def create_qualifiers(self, rel):

        qualifiers = []

        # Genome-wide association study (Q1098876)
        qualifiers.append(wdi_core.WDItemID(value='Q1098876', prop_nr=PROPS['determination method'], is_qualifier=True))

        # TAS (Q23190853)
        qualifiers.append(
            wdi_core.WDItemID(value='Q23190853', prop_nr=PROPS['determination method'], is_qualifier=True))

        return qualifiers


if __name__ == "__main__":
    """
    Bot to add/update gene-disease relationships to wikidata.
    Data from: https://gemma.msl.ubc.ca/phenocarta/LatestEvidenceExport/AnnotationsByDataset/GWAS_Catalog.tsv
    """
    parser = argparse.ArgumentParser(description='run wikidata gene-disease bot')
    parser.add_argument("path", help="path to GWAS_Catalog.tsv")
    parser.add_argument('--dummy', help='do not actually do write', action='store_true')
    parser.add_argument('--fastrun', dest='fastrun', action='store_true')
    parser.add_argument('--no-fastrun', dest='fastrun', action='store_false')
    parser.set_defaults(fastrun=True)

    args = parser.parse_args()
    path = args.path

    assert os.path.exists(path), "Cannot find {}".format(path)

    last_modified = datetime.fromtimestamp(os.path.getmtime(path), pytz.timezone("UTC"))

    run_id = datetime.now().strftime('%Y%m%d_%H:%M')
    __metadata__['run_id'] = run_id
    __metadata__['sources'] = [{'name': 'Phenocarta', 'version': str(last_modified)}]
    fast_run = args.fastrun

    log_name = '{}-{}.log'.format(__metadata__['name'], run_id)
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []
    wdi_core.WDItemEngine.setup_logging(log_name=log_name, header=json.dumps(__metadata__), logger_name='gene-disease')

    login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS)
    GeneDiseaseBot(catalog_tsv_path=path, login=login, fast_run=fast_run, write=not args.dummy).run()
