"""
example human gene
https://www.wikidata.org/wiki/Q14911732
https://mygene.info/v3/gene/1017
https://www.ncbi.nlm.nih.gov/gene/1017
http://uswest.ensembl.org/Homo_sapiens/Gene/Summary?g=ENSG00000123374;r=12:55966769-55972784

example mouse gene
https://www.wikidata.org/wiki/Q21129787

example yeast gene:
https://www.wikidata.org/wiki/Q27539933
https://mygene.info/v3/gene/856615

example microbial gene:
https://www.wikidata.org/wiki/Q23097138
https://mygene.info/v3/gene/7150837

Restructuring this: https://bitbucket.org/sulab/wikidatabots/src/226614eeda5f258fc913b10fdcaa3c22c7f64045/automated_bots/genes/mammals/gene.py?at=jenkins-automation&fileviewer=file-view-default



"""
#TODO: Gene on two chromosomes
#https://www.wikidata.org/wiki/Q20787772

## TODO: Homologues
# do this in another bot because the items might not exist yet

import argparse
import json
import os
import sys
import traceback
from datetime import datetime

import pymongo
from pymongo import MongoClient
from scheduled_bots.geneprotein import HelperBot
from scheduled_bots.geneprotein import type_of_gene_map, organisms_info
from scheduled_bots.geneprotein.ChromosomeBot import ChromosomeBot
from scheduled_bots.geneprotein.HelperBot import make_ref_source
from tqdm import tqdm
from wikidataintegrator import wdi_login, wdi_core, wdi_helpers

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
         'strand orientation': 'P2548',
         'Entrez Gene ID': 'P351',
         'NCBI Locus tag': 'P2393',
         'Ensembl Gene ID': 'P594',
         'Ensembl Transcript ID': 'P704',
         'genomic assembly': 'P659',
         'genomic start': 'P644',
         'genomic end': 'P645',
         'chromosome': 'P1057',
         'Saccharomyces Genome Database ID': 'P3406',
         'Mouse Genome Informatics ID': 'P671',
         'HGNC ID': 'P354',
         'HGNC Gene Symbol': 'P353',
         'RefSeq RNA ID': 'P639',
         'HomoloGene ID': 'P593'
         }

__metadata__ = {'name': 'GeneBot',
                'maintainer': 'GSS',
                'tags': ['gene'],
                'properties': list(PROPS.values())
                }

# If the source is "entrez", the reference identifier to be used is "Ensembl Gene ID" (P594)
source_ref_id = {'ensembl': "Ensembl Gene ID",
                 'entrez': 'Entrez Gene ID',
                 'uniprot': None}

class Gene:
    """
    Generic gene class. Subclasses: Human, Mammal, Microbe
    """
    record = None
    label = None
    description = None
    aliases = None
    external_ids = None

    def __init__(self, record, organism_info, login, write=True):
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
        self.login = login
        self.write = write

        self.statements = None

    def create_description(self):
        self.description = '{} gene found in {}'.format(self.organism_info['type'], self.organism_info['name'])

    def create_label(self):
        self.label = self.record['name']['@value']

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

        entrez_gene = str(self.record['entrezgene']['@value'])

        ensembl_gene = self.record['ensembl']['@value']['gene']

        external_ids = {'Entrez Gene ID': entrez_gene, 'Ensembl Gene ID': ensembl_gene}

        ############
        # optional external IDs
        ############
        if 'locus_tag' in self.record:
            # ncbi locus tag
            external_ids['NCBI Locus tag'] = self.record['locus_tag']['@value']

        if 'SGD' in self.record:
            external_ids['Saccharomyces Genome Database ID'] = self.record['SGD']['@value']

        if 'HGNC' in self.record:
            external_ids['HGNC ID'] = self.record['HGNC']['@value']

        if 'symbol' in self.record and 'HGNC' in self.record:
            # "and 'HGNC' in record" is required because there is something wrong with mygene
            # see: https://github.com/stuppie/scheduled-bots/issues/2
            external_ids['HGNC Gene Symbol'] = self.record['symbol']['@value']

        if 'MGI' in self.record:
            external_ids['Mouse Genome Informatics ID'] = self.record['MGI']['@value']

        if 'homologene' in self.record:
            external_ids['HomoloGene ID'] = self.record['homologene']['@value']

        ############
        # optional external IDs (can have more than one)
        ############
        # Ensembl Transcript ID
        if 'transcript' in self.record['ensembl']['@value']:
            external_ids['Ensembl Transcript ID'] = self.record['ensembl']['@value']['transcript']

        # RefSeq RNA ID
        if 'rna' in self.record['refseq']['@value']:
            external_ids['RefSeq RNA ID'] = self.record['refseq']['@value']['rna']

        self.external_ids = external_ids

    def create_statements(self):
        """
        create statements common to all genes
        """
        s = []

        ############
        # ID statements
        ############
        ensembl_ref = make_ref_source(self.record['ensembl']['@source'], PROPS['Ensembl Gene ID'],
                                      self.external_ids['Ensembl Gene ID'], login=self.login)
        entrez_ref = make_ref_source(self.record['entrezgene']['@source'], PROPS['Entrez Gene ID'],
                                     self.external_ids['Entrez Gene ID'], login=self.login)

        s.append(wdi_core.WDString(self.external_ids['Entrez Gene ID'], PROPS['Entrez Gene ID'], references=[entrez_ref]))
        s.append(wdi_core.WDString(self.external_ids['Ensembl Gene ID'], PROPS['Ensembl Gene ID'], references=[ensembl_ref]))

        # optional ID statements
        key = 'Ensembl Transcript ID'
        if key in self.external_ids:
            for id in self.external_ids[key]:
                ref = make_ref_source(self.record['ensembl']['@source'], PROPS[key], id, login=self.login)
                s.append(wdi_core.WDString(id, PROPS[key], references=[ref]))

        key = 'RefSeq RNA ID'
        if key in self.external_ids:
            for id in self.external_ids[key]:
                ref = make_ref_source(self.record['refseq']['@source'], PROPS[key], id, login=self.login)
                s.append(wdi_core.WDString(id, PROPS[key], references=[ref]))

        for key in ['NCBI Locus tag', 'Saccharomyces Genome Database ID', 'Mouse Genome Informatics ID']:
            if key in self.external_ids:
                s.append(wdi_core.WDString(self.external_ids[key], PROPS[key], references=[entrez_ref]))

        ############
        # Gene statements
        ############
        # subclass of gene/protein-coding gene/etc
        type_of_gene = self.record['type_of_gene']['@value']
        s.append(wdi_core.WDItemID(type_of_gene_map[type_of_gene], PROPS['subclass of'], references=[ensembl_ref]))

        # found in taxon
        s.append(wdi_core.WDItemID(self.organism_info['wdid'], PROPS['found in taxon'], references=[ensembl_ref]))

        return s

    def create_item(self):
        self.parse_external_ids()
        self.statements = self.create_statements()
        self.create_label()
        self.create_description()

        try:
            wd_item_gene = wdi_core.WDItemEngine(item_name=self.label, domain='genes', data=self.statements,
                                                 append_value=[PROPS['subclass of']],
                                                 fast_run=False,
                                                 fast_run_base_filter={PROPS['Entrez Gene ID']: '',
                                                                       PROPS['found in taxon']: self.organism_info['wdid']})
            self.set_languages(wd_item_gene)
            wd_item_gene.set_aliases(self.aliases)
            wdi_helpers.try_write(wd_item_gene, self.external_ids['Entrez Gene ID'], PROPS['Entrez Gene ID'], self.login,
                                  write=self.write)
        except Exception as e:
            exc_info = sys.exc_info()
            traceback.print_exception(*exc_info)
            msg = wdi_helpers.format_msg(self.external_ids['Entrez Gene ID'], PROPS['Entrez Gene ID'], None,
                                         str(e), msg_type=type(e))
            wdi_core.WDItemEngine.log("ERROR", msg)

    def set_languages(self, wditem):
        pass


class MicrobeGene(Gene):
    """
    Microbes

    """
    def __init__(self, record, organism_info, chr_num_wdid, login):
        super().__init__(record, organism_info, chr_num_wdid, login)

    def create_description(self):
        self.description = '{} gene found in {}'.format(self.organism_info['type'], self.organism_info['name'])

    def create_label(self):
        self.label = self.record['name']['@value']

    def create_statements(self):

        # create generic gene statements
        s = super().create_statements()

        # add on gene position statements
        s.extend(self.create_gp_statements())

        return s

    def create_gp_statements(self):
        """
        Create genomic_pos start stop orientation no chromosome
        :return:
        """
        genomic_pos_value = self.record['genomic_pos']['@value']
        genomic_pos_source = self.record['genomic_pos']['@source']
        genomic_pos_id_prop = source_ref_id[genomic_pos_source['id']]
        genomic_pos_ref = make_ref_source(genomic_pos_source, PROPS[genomic_pos_id_prop],
                                          self.external_ids[genomic_pos_id_prop], login=self.login)

        s = []
        # strand orientation
        strand_orientation = 'Q22809680' if genomic_pos_value['strand'] == 1 else 'Q22809711'
        s.append(wdi_core.WDItemID(strand_orientation, PROPS['strand orientation'], references=[genomic_pos_ref]))
        # genomic start and end
        s.append(wdi_core.WDString(str(int(genomic_pos_value['start'])), PROPS['genomic start'],
                                   references=[genomic_pos_ref]))
        s.append(wdi_core.WDString(str(int(genomic_pos_value['end'])), PROPS['genomic end'],
                                   references=[genomic_pos_ref]))

        return s


class MammalianGene(Gene):
    """
    Probably should be called euakaryotes. includes yeast
    """
    gene_of_the_species = {'en': "gene of the species {}",
                           'fr': "gène de l'espèce {}",
                           'nl': "gen van de soort {}",
                           'de': "Gen der Spezies {}",
                           'es': "gen de la especie {}",
                           'pt': "gene da espécie {}",
                           'sv': "Genen från arten {}",
                           'srn': "gen fu a sortu {}",
                           }
    # languages to set the gene label to
    label_languages = gene_of_the_species.keys()

    def __init__(self, record, organism_info, chr_num_wdid, login, write=True):
        """
        :param chr_num_wdid: mapping of chr number (str) to wdid
        """
        super().__init__(record, organism_info, login)
        self.chr_num_wdid = chr_num_wdid

    def create_description(self):
        self.description = 'gene of the species {}'.format(self.organism_info['name'])

    def create_label(self):
        self.label = self.record['symbol']['@value']

    def set_languages(self, wditem):
        for lang in self.label_languages:
            wditem.set_label(self.label, lang=lang)
        for lang, desc in self.gene_of_the_species.items():
            if wditem.get_description(lang=lang) == "":
                wditem.set_description(desc.format(self.organism_info['name']), lang=lang)

    def create_statements(self):

        # create generic gene statements
        s = super().create_statements()

        # add on gene position statements
        s.extend(self.create_gp_statements_chr())

        return s

    def create_gp_statements_chr(self):
        """
        Create genomic_pos start stop orientation on a chromosome
        :return:
        """
        genomic_pos_value = self.record['genomic_pos']['@value']
        genomic_pos_source = self.record['genomic_pos']['@source']
        genomic_pos_id_prop = source_ref_id[genomic_pos_source['id']]
        genomic_pos_ref = make_ref_source(genomic_pos_source, PROPS[genomic_pos_id_prop],
                                          self.external_ids[genomic_pos_id_prop], login=self.login)

        # create qualifier for start/stop/orientation
        chrom_wdid = self.chr_num_wdid[genomic_pos_value['chr']]
        qualifiers = [wdi_core.WDItemID(chrom_wdid, PROPS['chromosome'], is_qualifier=True)]

        s = []
        # strand orientation
        strand_orientation = 'Q22809680' if genomic_pos_value['strand'] == 1 else 'Q22809711'
        s.append(wdi_core.WDItemID(strand_orientation, PROPS['strand orientation'], references=[genomic_pos_ref]))
        # genomic start and end
        s.append(wdi_core.WDString(str(int(genomic_pos_value['start'])), PROPS['genomic start'],
                                   references=[genomic_pos_ref], qualifiers=qualifiers))
        s.append(wdi_core.WDString(str(int(genomic_pos_value['end'])), PROPS['genomic end'],
                                   references=[genomic_pos_ref], qualifiers=qualifiers))
        # chromosome
        s.append(wdi_core.WDItemID(chrom_wdid, PROPS['chromosome'], references=[genomic_pos_ref]))

        return s


class HumanGene(MammalianGene):

    def create_statements(self):
        # create all mammalian gene statements
        s = super().create_statements()
        entrez_ref = make_ref_source(self.record['entrezgene']['@source'], PROPS['Entrez Gene ID'],
                                     self.external_ids['Entrez Gene ID'], login=self.login)

        # add on human specific gene statements
        for key in ['HGNC ID', 'HGNC Gene Symbol']:
            s.append(wdi_core.WDString(self.external_ids[key], PROPS[key], references=[entrez_ref]))

        # add on gene position statements
        s.extend(self.do_gp_human())

        return s

    def validate_record(self):
        assert 'locus_tag' in self.record
        assert 'HGNC' in self.record
        assert 'symbol' in self.record
        assert 'ensembl' in self.record and 'transcript' in self.record['ensemb']
        assert 'refseq' in self.record and 'rna' in self.record['ensemb']
        assert 'alias' in self.record

    def do_gp_human(self):
        """
        create genomic pos, chr, strand statements for human
        includes genomic assembly
        :return:
        """
        genomic_pos_value = self.record['genomic_pos']['@value']
        genomic_pos_source = self.record['genomic_pos']['@source']
        genomic_pos_id_prop = source_ref_id[genomic_pos_source['id']]
        genomic_pos_ref = make_ref_source(genomic_pos_source, PROPS[genomic_pos_id_prop],
                                          self.external_ids[genomic_pos_id_prop], login=self.login)
        assembly = wdi_core.WDItemID("Q20966585", PROPS['genomic assembly'], is_qualifier=True)

        # create qualifier for start/stop
        chrom_wdid = self.chr_num_wdid[genomic_pos_value['chr']]
        qualifiers = [wdi_core.WDItemID(chrom_wdid, PROPS['chromosome'], is_qualifier=True), assembly]

        strand_orientation = 'Q22809680' if genomic_pos_value['strand'] == 1 else 'Q22809711'

        if 'genomic_pos_hg19' in self.record:
            do_hg19 = True
            genomic_pos_value_hg19 = self.record['genomic_pos_hg19']['@value']
            genomic_pos_source_hg19 = self.record['genomic_pos_hg19']['@source']
            genomic_pos_id_prop_hg19 = source_ref_id[genomic_pos_source_hg19['id']]
            genomic_pos_ref_hg19 = make_ref_source(genomic_pos_source_hg19, PROPS[genomic_pos_id_prop_hg19],
                                                   self.external_ids[genomic_pos_id_prop_hg19], login=self.login)
            assembly_hg19 = wdi_core.WDItemID("Q21067546", PROPS['genomic assembly'], is_qualifier=True)
            chrom_wdid_hg19 = self.chr_num_wdid[genomic_pos_value_hg19['chr']]
            qualifiers_hg19 = [wdi_core.WDItemID(chrom_wdid_hg19, PROPS['chromosome'], is_qualifier=True),
                               assembly_hg19]
            strand_orientation_hg19 = 'Q22809680' if genomic_pos_value_hg19['strand'] == 1 else 'Q22809711'
        else:
            do_hg19 = False
            strand_orientation_hg19 = None
            assembly_hg19 = None
            genomic_pos_ref_hg19 = None
            genomic_pos_value_hg19 = None
            qualifiers_hg19 = None
            chrom_wdid_hg19 = None

        s = []

        # strand orientation
        # if the same for both assemblies, only put one statement
        if do_hg19 and strand_orientation == strand_orientation_hg19:
            s.append(wdi_core.WDItemID(strand_orientation, PROPS['strand orientation'],
                                       references=[genomic_pos_ref], qualifiers=[assembly, assembly_hg19]))
        else:
            s.append(wdi_core.WDItemID(strand_orientation, PROPS['strand orientation'],
                                       references=[genomic_pos_ref], qualifiers=[assembly]))
            if do_hg19:
                s.append(wdi_core.WDItemID(strand_orientation_hg19, PROPS['strand orientation'],
                                           references=[genomic_pos_ref_hg19], qualifiers=[assembly_hg19]))

        # genomic start and end for both assemblies
        s.append(wdi_core.WDString(str(int(genomic_pos_value['start'])), PROPS['genomic start'],
                                   references=[genomic_pos_ref], qualifiers=qualifiers))
        s.append(wdi_core.WDString(str(int(genomic_pos_value['end'])), PROPS['genomic end'],
                                   references=[genomic_pos_ref], qualifiers=qualifiers))
        if do_hg19:
            s.append(wdi_core.WDString(str(int(genomic_pos_value_hg19['start'])), PROPS['genomic start'],
                                       references=[genomic_pos_ref_hg19], qualifiers=qualifiers_hg19))
            s.append(wdi_core.WDString(str(int(genomic_pos_value_hg19['end'])), PROPS['genomic end'],
                                       references=[genomic_pos_ref_hg19], qualifiers=qualifiers_hg19))

        # chromosome
        # if the same for both assemblies, only put one statement
        if do_hg19 and chrom_wdid == chrom_wdid_hg19:
            s.append(wdi_core.WDItemID(chrom_wdid, PROPS['chromosome'],
                                       references=[genomic_pos_ref], qualifiers=[assembly, assembly_hg19]))
        else:
            s.append(wdi_core.WDItemID(chrom_wdid, PROPS['chromosome'],
                                       references=[genomic_pos_ref], qualifiers=[assembly]))
            if do_hg19:
                s.append(wdi_core.WDItemID(chrom_wdid_hg19, PROPS['chromosome'],
                                           references=[genomic_pos_ref_hg19], qualifiers=[assembly_hg19]))

        return s


class GeneBot:
    """
    Generic genebot class
    """
    GENE_CLASS = Gene

    def __init__(self, organism_info, login):
        self.login = login
        self.organism_info = organism_info

    def run(self, records, total=None, write=True):
        for record in tqdm(records, mininterval=2, total=total):
            gene = self.GENE_CLASS(record, self.organism_info, self.login, write=write)
            gene.create_item()


class MammalianGeneBot(GeneBot):
    GENE_CLASS = MammalianGene

    def __init__(self, organism_info, chr_num_wdid, login):
        super().__init__(organism_info, login)
        self.chr_num_wdid = chr_num_wdid

    def run(self, records, total=None, write=True):
        for record in tqdm(records, mininterval=2, total=total):
            gene = self.GENE_CLASS(record, self.organism_info, self.chr_num_wdid, self.login, write=write)
            gene.create_item()

class HumanGeneBot(MammalianGeneBot):
    GENE_CLASS = HumanGene


def main(coll: pymongo.collection.Collection, taxid: str, log_dir: str = "./logs", write: bool = True) -> None:
    """
    Main function for creating/updating genes

    :param coll: mongo collection containing gene data from mygene
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
    if organism_info['type'] in {"fungal", "mammalian"}:
        # make sure all chromosome items are found in wikidata
        cb = ChromosomeBot()
        chr_num_wdid = cb.get_or_create(organism_info)
        if int(organism_info['taxid']) == 9606:
            bot = HumanGeneBot(organism_info, chr_num_wdid, login)
        else:
            bot = MammalianGeneBot(organism_info, chr_num_wdid, login)
    elif organism_info['type'] in {"microbial"}:
        bot = GeneBot(organism_info, login)
    else:
        raise ValueError("unknown organism")

    # only do certain records
    docs = coll.find({'taxid': taxid, 'type_of_gene': 'protein-coding'})
    total = docs.count()
    docs = HelperBot.validate_docs(docs, PROPS['Entrez Gene ID'])
    records = HelperBot.tag_mygene_docs(docs)

    bot.run(records, total=total)






if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='run wikidata gene bot')
    parser.add_argument('--log-dir', help='directory to store logs', type=str)
    parser.add_argument('--dummy', help='do not actually do write', action='store_true')
    parser.add_argument('--taxon', help="only run using this taxon (ncbi tax id)", type=str)
    parser.add_argument('--mongo-uri', type=str, default="mongodb://localhost:27017")
    parser.add_argument('--mongo-db', type=str, default="wikidata_src")
    parser.add_argument('--mongo-coll', type=str, default="mygene")
    args = parser.parse_args()
    log_dir = args.log_dir if args.log_dir else "./logs"
    run_id = datetime.now().strftime('%Y%m%d_%H:%M')
    taxon = args.taxon
    coll = MongoClient(args.mongo_uri)[args.mongo_db][args.mongo_coll]

    log_name = '{}-{}.log'.format(__metadata__['name'], datetime.now().strftime('%Y%m%d_%H:%M'))
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(__metadata__),
                                        logger_name='gene{}'.format(taxon))

    main(coll, taxon, log_dir=log_dir, write=not args.dummy)
