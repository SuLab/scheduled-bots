"""

example human protein
https://www.wikidata.org/wiki/Q511968
https://mygene.info/v3/gene/1017
https://www.ncbi.nlm.nih.gov/gene/1017
http://uswest.ensembl.org/Homo_sapiens/Gene/Summary?g=ENSG00000123374;r=12:55966769-55972784

example mouse protein
https://www.wikidata.org/wiki/Q14911733

example yeast protein:
https://www.wikidata.org/wiki/Q27547285
https://mygene.info/v3/gene/856615

example microbial protein:
https://www.wikidata.org/wiki/Q22161590
https://mygene.info/v3/gene/7150837

"""

import argparse
import json
import os
import sys
import traceback
from datetime import datetime

import pymongo
from pymongo import MongoClient
from scheduled_bots.geneprotein import HelperBot
from scheduled_bots.geneprotein import organisms_info
from scheduled_bots.geneprotein.HelperBot import make_ref_source
from tqdm import tqdm
from wikidataintegrator import wdi_login, wdi_core, wdi_helpers
from wikidataintegrator.wdi_helpers import id_mapper

try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

PROPS = {'found in taxon': 'P703',
         'subclass of': 'P279',
         'encoded by': 'P702',
         'RefSeq Protein ID': 'P637',
         'UniProt ID': 'P352',
         'Ensembl Protein ID': 'P705',
         'OMIM ID': 'P492',
         'Entrez Gene ID': 'P351',

         'Saccharomyces Genome Database ID': 'P3406',
         'Mouse Genome Informatics ID': 'P671',
         }

__metadata__ = {'name': 'ProteinBot',
                'maintainer': 'GSS',
                'tags': ['protein'],
                'properties': list(PROPS.values())
                }

# If the source is "entrez", the reference identifier to be used is "Ensembl Gene ID" (P594)
source_ref_id = {'ensembl': "Ensembl Protein ID",
                 'entrez': 'Entrez Gene ID',
                 'uniprot': 'UniProt ID'}


class Protein:
    """
    Generic protein class. For microbes and yeast
    """
    record = None
    label = None
    description = None
    aliases = None
    external_ids = None

    def __init__(self, record, organism_info, gene_wdid, login):
        """
        generate pbb_core item object

        :param record: dict from mygene,tagged with @value and @source
        :param organism_info: looks like {
            "type": "fungal",
            "name": "Saccharomyces cerevisiae S288c",
            "wdid": "Q27510868",
            'taxid': 559292
        }

        :param login:

        """
        self.record = record
        self.organism_info = organism_info
        self.gene_wdid = gene_wdid
        self.login = login

        self.statements = None

    def create_description(self):
        self.description = '{} protein found in {}'.format(self.organism_info['type'], self.organism_info['name'])

    def create_label(self):
        self.label = self.record['name']['@value']
        if 'locus_tag' in self.record:
            self.label += " " + self.record['locus_tag']['@value']
        self.label = self.label[0].upper() + self.label[1:]

    def create_aliases(self):
        aliases = [self.record['symbol']['@value']]
        if 'NCBI Locus tag' in self.external_ids:
            aliases.append(self.external_ids['NCBI Locus tag'])
        if 'alias' in self.record:
            aliases.extend(self.record['alias'])
        self.aliases = aliases

    def validate_record(self):
        # handled by HelperBot
        # allow for subclasses to add additional checks
        pass

    def parse_external_ids(self):
        ############
        # required external IDs
        ############
        uniprot_id = self.record['uniprot']['@value']['Swiss-Prot']
        entrez_gene = str(self.record['entrezgene']['@value'])

        external_ids = {'UniProt ID': uniprot_id, 'Entrez Gene ID': entrez_gene}

        ############
        # optional external IDs
        ############
        # SGD on both gene and protein item
        if 'SGD' in self.record:
            external_ids['Saccharomyces Genome Database ID'] = self.record['SGD']['@value']

        ############
        # optional external IDs (can have more than one)
        ############
        if 'ensembl' in self.record and 'protein' in self.record['ensembl']['@value']:
            # Ensembl Protein ID
            external_ids['Ensembl Protein ID'] = self.record['ensembl']['@value']['protein']

        if 'refseq' in self.record and 'protein' in self.record['refseq']['@value']:
            # RefSeq Protein ID
            external_ids['RefSeq Protein ID'] = self.record['refseq']['@value']['protein']

        self.external_ids = external_ids

    def create_statements(self):
        """
        create statements common to all proteins
        """
        s = []

        ############
        # ID statements
        # Required: uniprot (1)
        # Optional: OMIM (1?), Ensembl protein (0 or more), refseq protein (0 or more)
        ############
        uniprot_ref = make_ref_source(self.record['uniprot']['@source'], PROPS['UniProt ID'],
                                      self.external_ids['UniProt ID'],
                                      login=self.login)
        entrez_ref = make_ref_source(self.record['entrezgene']['@source'], PROPS['Entrez Gene ID'],
                                     self.external_ids['Entrez Gene ID'], login=self.login)

        s.append(wdi_core.WDString(self.external_ids['UniProt ID'], PROPS['UniProt ID'], references=[uniprot_ref]))

        for key in ['Saccharomyces Genome Database ID']:
            if key in self.external_ids:
                s.append(wdi_core.WDString(self.external_ids[key], PROPS[key], references=[entrez_ref]))

        key = 'Ensembl Protein ID'
        if key in self.external_ids:
            for id in self.external_ids[key]:
                ref = make_ref_source(self.record['ensembl']['@source'], PROPS[key], id, login=self.login)
                s.append(wdi_core.WDString(id, PROPS[key], references=[ref]))

        key = 'RefSeq Protein ID'
        if key in self.external_ids:
            for id in self.external_ids[key]:
                ref = make_ref_source(self.record['refseq']['@source'], PROPS['Entrez Gene ID'], id, login=self.login)
                s.append(wdi_core.WDString(id, PROPS[key], references=[ref]))

        ############
        # Protein statements
        ############
        # subclass of protein
        s.append(wdi_core.WDItemID("Q8054", PROPS['subclass of'], references=[uniprot_ref]))

        # found in taxon
        s.append(wdi_core.WDItemID(self.organism_info['wdid'], PROPS['found in taxon'], references=[uniprot_ref]))

        # encoded by
        s.append(wdi_core.WDItemID(self.gene_wdid, PROPS['encoded by'], references=[entrez_ref]))

        return s

    def create_item(self, write=True):
        try:
            self.parse_external_ids()
            self.statements = self.create_statements()
            self.create_label()
            self.create_description()
            self.create_aliases()
            wd_item_protein = wdi_core.WDItemEngine(item_name=self.label, domain='proteins', data=self.statements,
                                                    append_value=[PROPS['subclass of']],
                                                    fast_run=False,
                                                    fast_run_base_filter={PROPS['UniProt ID']: '',
                                                                          PROPS['found in taxon']: self.organism_info['wdid']})
            wd_item_protein.set_label(self.label)
            wd_item_protein.set_description(self.description, lang='en')
            wd_item_protein.set_aliases(self.aliases)
            wdi_helpers.try_write(wd_item_protein, self.external_ids['UniProt ID'], PROPS['UniProt ID'], self.login,
                                  write=write)
        except Exception as e:
            exc_info = sys.exc_info()
            traceback.print_exception(*exc_info)
            msg = wdi_helpers.format_msg(self.external_ids['UniProt ID'], PROPS['UniProt ID'], None,
                                         str(e), msg_type=type(e))
            wdi_core.WDItemEngine.log("ERROR", msg)


class ProteinBot:
    """
    Generic proteinbot class
    """

    def __init__(self, organism_info, gene_wdid_mapping, login):
        self.login = login
        self.organism_info = organism_info
        self.gene_wdid_mapping = gene_wdid_mapping

    def run(self, records, total=None, write=True):
        for record in tqdm(records, mininterval=2, total=total):
            gene_wdid = self.gene_wdid_mapping[str(record['entrezgene']['@value'])]
            protein = Protein(record, self.organism_info, gene_wdid, self.login)
            protein.create_item(write=write)


def main(coll: pymongo.collection.Collection, taxid: str, metadata, log_dir: str = "./logs",
         write: bool = True) -> None:
    """
    Main function for creating/updating proteins

    :param coll: mongo collection containing protein data from mygene
    :param taxid: taxon to use (ncbi tax id)
    :param log_dir: dir to store logs
    :param write: actually perform write
    :return:
    """

    # make sure the organism is found in wikidata
    taxid = int(taxid)
    organism_wdid = wdi_helpers.prop2qid("P685", taxid)
    if not organism_wdid:
        raise ValueError("organism {} not found".format(taxid))

    # login
    login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS)
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(__metadata__))

    organism_info = organisms_info[taxid]
    # get all entrez gene id -> wdid mappings, where found in taxon is this strain
    gene_wdid_mapping = id_mapper("P351", (("P703", organism_info['wdid']),))

    bot = ProteinBot(organism_info, gene_wdid_mapping, login)

    # only do certain records
    docs = coll.find({'taxid': taxid, 'type_of_gene': 'protein-coding', 'uniprot.Swiss-Prot': {'$exists': True}}).batch_size(20)
    total = docs.count()
    docs = HelperBot.validate_docs(docs, 'protein', PROPS['Entrez Gene ID'])
    records = HelperBot.tag_mygene_docs(docs, metadata)

    bot.run(records, total=total, write=write)


if __name__ == "__main__":
    """
    Data to be used is stored in a mongo collection. collection name: "mygene"
    """
    parser = argparse.ArgumentParser(description='run wikidata protein bot')
    parser.add_argument('--log-dir', help='directory to store logs', type=str)
    parser.add_argument('--dummy', help='do not actually do write', action='store_true')
    parser.add_argument('--taxon', help="only run using this taxon (ncbi tax id)", type=str)
    parser.add_argument('--mongo-uri', type=str, default="mongodb://localhost:27017")
    parser.add_argument('--mongo-db', type=str, default="wikidata_src")
    args = parser.parse_args()
    log_dir = args.log_dir if args.log_dir else "./logs"
    run_id = datetime.now().strftime('%Y%m%d_%H:%M')
    taxon = args.taxon
    coll = MongoClient(args.mongo_uri)[args.mongo_db]["mygene"]

    # get metadata about sources
    # this should be stored in the same db under the collection: mygene_sources
    metadata_coll = MongoClient(args.mongo_uri)[args.mongo_db]["mygene_sources"]
    assert metadata_coll.count() == 1
    metadata = metadata_coll.find_one()

    log_name = '{}-{}.log'.format(__metadata__['name'], datetime.now().strftime('%Y%m%d_%H:%M'))
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(__metadata__),
                                        logger_name='protein{}'.format(taxon))

    main(coll, taxon, metadata, log_dir=log_dir, write=not args.dummy)
