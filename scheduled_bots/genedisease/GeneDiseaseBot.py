import argparse
import json
import os
import sys
import traceback
import urllib.request
from collections import defaultdict, Counter
from datetime import datetime
from itertools import chain
from time import gmtime, strftime

import requests
from wikidataintegrator import wdi_core, wdi_helpers, wdi_login
from wikidataintegrator.wdi_helpers import id_mapper

try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

PROPS = {'Entrez Gene ID': 'P351',
         'Disease Ontology ID': 'P699',
         'genetic association': 'P2293',
         'reference URL': 'P854',
         'stated in': 'P248',
         'retrieved': 'P813',
         'determination method': 'P459',
         'found in taxon': 'P703',
         'PubMed ID': 'P698',
         }

__metadata__ = {'name': 'GeneDiseaseBot',
                'tags': ['gene', 'disease'],
                'properties': list(PROPS.values())
                }


class GWASCatalog(object):

    def __init__(self, catalog_tsv_path='GWAS_Catalog.tsv'):
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

    phenocarta_ref_source = 'http://chibi.ubc.ca/Gemma/phenotypes.html?phenotypeUrlId=DOID_{doid}&ncbiId={ncbi}'

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
        return not(self == other)


class GeneDiseaseBot(object):

    def __init__(self, catalog_tsv_path='GWAS_Catalog.tsv', login=None, fast_run=False, write=True):
        self.gwas_catalog = GWASCatalog(catalog_tsv_path=catalog_tsv_path)

        self.fast_run_base_gene_filter = {PROPS['Entrez Gene ID']: ""}
        self.fast_run_base_disease_filter = {PROPS['Disease Ontology ID']: ""}
        self.login = login
        self.write = write
        self.fast_run = fast_run

        # Load doid -> wdid mapper
        self.doid_wdid_map = id_mapper(PROPS['Disease Ontology ID'])

        # Load entrez gene -> wdid mapper
        self.gene_wdid_map = id_mapper(PROPS['Entrez Gene ID'], filters=[(PROPS['found in taxon'], 'Q15978631')])

        # Load pmid -> wdid mapper
        self.pmid_wdid_map = id_mapper(PROPS['PubMed ID'])

        self.wd_items = []

    def run(self):
        wdi_core.WDItemEngine.log("INFO", "Begin processing relationships")
        wd_genes = defaultdict(list)
        wd_diseases = defaultdict(list)
        for i, gdr in enumerate(self.gwas_catalog.data, start=1):
            if i % 50 == 0:
                wdi_core.WDItemEngine.log("DEBUG", "Processing Relationships: {} / {}".format(i, len(self.gwas_catalog.data)))

            try:
                # Retrieve Wikidata ID for this disease phenotype
                doid_wdid = self.doid_wdid_map["DOID:{}".format(gdr.doid)]
            except KeyError as e:
                msg = "Missing DOID Disease WD Item; skipping {}".format(gdr.doid)
                print(msg)
                wdi_core.WDItemEngine.log("ERROR", wdi_helpers.format_msg(gdr.doid, PROPS['Disease Ontology ID'], None, msg, type(e)))
                continue

            try:
                # Retrieve Wikidata ID for this gene
                gene_wdid = self.gene_wdid_map[gdr.ncbi]
            except KeyError as e:
                msg = "Missing NCBI Gene WD Item; skipping {}".format(gdr.ncbi)
                print(msg)
                wdi_core.WDItemEngine.log("ERROR", wdi_helpers.format_msg(gdr.ncbi, PROPS['Entrez Gene ID'], None, msg, type(e)))
                continue

            items = self.process_relationship(gene_wdid, doid_wdid, gdr)

            gdr.gene_wditem = items['gene_item']
            gdr.disease_wditem = items['disease_item']

            # Aggregating data to reduce wikidata updates
            wd_genes[gene_wdid].append(gdr)
            wd_diseases[doid_wdid].append(gdr)
        wdi_core.WDItemEngine.log("DEBUG", "Processing Relationships: {} / {}".format(i, len(self.gwas_catalog.data)))

        wdi_core.WDItemEngine.log("INFO", "Begin creating Wikidata items with new relationships")
        self.wd_items = []
        # Create Wikidata items for genes
        for i, (wdid, gdrs) in enumerate(wd_genes.items(), start=1):
            if i % 100 == 0:
                wdi_core.WDItemEngine.log("DEBUG", "Creating Wikidata Gene Items: {} / {}".format(i, len(wd_genes)))
            # Attach updated disease information to gene
            try:
                gene_wd_item = wdi_core.WDItemEngine(wd_item_id=wdid,
                                                     data=[gdr.disease_wditem for gdr in gdrs],
                                                     domain="genes",
                                                     append_value=[PROPS["genetic association"]],
                                                     fast_run=self.fast_run,
                                                     fast_run_base_filter=self.fast_run_base_gene_filter)
                self.wd_items.append({'item': gene_wd_item, 'record_id': gdrs[0].ncbi, 'record_prop': PROPS['Entrez Gene ID']})
            except Exception as e:
                msg = "Problem Creating Gene WDItem; skipping {}".format(gdr.ncbi)
                wdi_core.WDItemEngine.log("ERROR", wdi_helpers.format_msg(gdr.ncbi, PROPS['Entrez Gene ID'], wdid, msg, type(e)))

        wdi_core.WDItemEngine.log("DEBUG", "Creating Wikidata Gene Items: {} / {}".format(i, len(wd_genes)))

        for i, (wdid, gdrs) in enumerate(wd_diseases.items(), start=1):
            if i % 10 == 0:
                wdi_core.WDItemEngine.log("DEBUG", "Creating Wikidata Disease Items: {} / {}".format(i, len(wd_diseases)))
            # Attach updated gene information to disease
            try:
                disease_wd_item = wdi_core.WDItemEngine(wd_item_id=wdid,
                                                        data=[gdr.gene_wditem for gdr in gdrs],
                                                        domain="diseases",
                                                        append_value=[PROPS["genetic association"]],
                                                        fast_run=self.fast_run,
                                                        fast_run_base_filter=self.fast_run_base_disease_filter)
                self.wd_items.append({'item': disease_wd_item, 'record_id': "DOID:{}".format(gdrs[0].doid), 'record_prop': PROPS['Disease Ontology ID']})
            except Exception as e:
                msg = "Problem Creating Disease WDItem; skipping {}".format(gdr.doid)
                wdi_core.WDItemEngine.log("ERROR", wdi_helpers.format_msg(gdr.doid, PROPS['Disease Ontology ID'], wdid, msg, type(e)))
        wdi_core.WDItemEngine.log("DEBUG", "Creating Wikidata Disease Items: {} / {}".format(i, len(wd_diseases)))
        wdi_core.WDItemEngine.log("INFO", "All Items Created")

        self.write_all()

        if self.write:
            wdi_core.WDItemEngine.log("INFO", "All Items Written")

    def write_item(self, wd_item):
        try:
            wdi_helpers.try_write(wd_item['item'], record_id=wd_item['record_id'], record_prop=wd_item['record_prop'], edit_summary='edit genetic association', login=self.login, write=self.write)
        except Exception as e:
            print(e)
            wdi_core.WDItemEngine.log("ERROR", wdi_helpers.format_msg(wd_item['record_id'], wd_item['record_prop'], wd_item['item'].wd_item_id, str(e), type(e)))

    def write_all(self):
        for wd_item in self.wd_items:
            self.write_item(wd_item)

    def process_relationship(self, gene_wdid, doid_wdid, gdr):
        """
        Process will involve creating disease items with references
        to the genes and vice-versa.
        """

        # Create updated references
        genetic_assoc_ref = self.create_references(gdr)
        qualifiers = self.create_qualifiers(gdr)

        # Attach the created genetic association references to this disease phenotype
        disease_item = wdi_core.WDItemID(value=doid_wdid, prop_nr=PROPS["genetic association"], references=[genetic_assoc_ref], qualifiers=qualifiers, check_qualifier_equality=False)

        # Repeat for attaching updated gene information to disease

        # Create updated references
        genetic_assoc_ref = self.create_references(gdr)
        qualifiers = self.create_qualifiers(gdr)

        # Attach the created genetic association references to this disease phenotype
        gene_item = wdi_core.WDItemID(value=gene_wdid, prop_nr=PROPS["genetic association"], references=[genetic_assoc_ref], qualifiers=qualifiers, check_qualifier_equality=False)

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
        references.append(wdi_core.WDItemID(value=self.get_or_create_article(gdr.pmid), prop_nr=PROPS['stated in'], is_reference=True))

        # Date retrieved
        references.append(wdi_core.WDTime(strftime("+%Y-%m-%dT00:00:00Z", gmtime()), prop_nr=PROPS['retrieved'], is_reference=True))

        return references

    def create_qualifiers(self, rel):

        qualifiers = []

        # Genome-wide association study (Q1098876)
        qualifiers.append(wdi_core.WDItemID(value='Q1098876', prop_nr=PROPS['determination method'], is_qualifier=True))

        # TAS (Q23190853)
        qualifiers.append(wdi_core.WDItemID(value='Q23190853', prop_nr=PROPS['determination method'], is_qualifier=True))

        return qualifiers

    def get_or_create_article(self, pmid):
        # check if exists in wikidata
        if pmid in self.pmid_wdid_map:
            return self.pmid_wdid_map[pmid]
        else:
            p = wdi_helpers.PubmedItem(pmid)
            if self.write:
                wdid = p.get_or_create(self.login)
            else:
                wdid = 'Q1'  # Dummy ID
                wdi_core.WDItemEngine.log("INFO", wdi_helpers.format_msg(pmid, PROPS['PubMed ID'], wdid, "CREATE"))
            self.pmid_wdid_map[pmid] = wdid

        return wdid


def main(gwas_path='GWAS_Catalog.tsv', log_dir="./logs", fast_run=False, write=True):
    login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS)
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, logger_name='WD_logger', log_name=log_name,
                                        header=json.dumps(__metadata__))

    gene_disease_bot = GeneDiseaseBot(catalog_tsv_path=gwas_path, login=login, fast_run=fast_run, write=write).run()


def download_gwas(url):
    """
    :param url: path to gwas catalog file to download
    :return:
    """
    # url = "http://www.chibi.ubc.ca/Gemma/phenocarta/LatestEvidenceExport/AnnotationsByDataset/GWAS_Catalog.tsv"
    r = requests.get(url, stream=True)
    r.raise_for_status()
    with open('GWAS_Catalog.tsv', 'wb') as handle:
        for block in r.iter_content(1024):
            handle.write(block)

if __name__ == "__main__":
    """
    Bot to add/update gene-disease relationships to wikidata.
    """
    parser = argparse.ArgumentParser(description='run wikidata gene-disease bot')
    parser.add_argument('--gwas-path', help='path to gwas catalog dump file')
    parser.add_argument('--gwas-url', help='url to gwas catalog dump file')
    parser.add_argument('--log-dir', help='directory to store logs', type=str)
    parser.add_argument('--dummy', help='do not actually do write', action='store_true')
    parser.add_argument('--fastrun', dest='fastrun', action='store_true')
    parser.add_argument('--no-fastrun', dest='fastrun', action='store_false')
    parser.set_defaults(fastrun=False)

    args = parser.parse_args()
    if (args.gwas_path and args.gwas_url) or not (args.gwas_path or args.gwas_url):
        raise ValueError("must give one of --gwas_path and --gwas_url")
    log_dir = args.log_dir if args.log_dir else "./logs"
    run_id = datetime.now().strftime('%Y%m%d_%H:%M')
    __metadata__['run_id'] = run_id
    fast_run = args.fastrun

    log_name = '{}-{}.log'.format(__metadata__['name'], run_id)
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(__metadata__),
                                        logger_name='gene-disease')

    gwas_path = args.gwas_path
    if args.gwas_url:
        download_gwas(args.gwas_url)
        gwas_path = "GWAS_Catalog.tsv"
    main(gwas_path, log_dir=log_dir, fast_run=fast_run, write=not args.dummy)
