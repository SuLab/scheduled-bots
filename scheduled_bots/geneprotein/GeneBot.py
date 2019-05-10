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

sparql query for listing current subclasses: http://tinyurl.com/y8ecgka7

"""
# TODO: Gene on two chromosomes
# https://www.wikidata.org/wiki/Q20787772

import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime
from functools import partial
from itertools import chain

from tqdm import tqdm

from scheduled_bots import get_default_core_props, PROPS
from scheduled_bots.geneprotein.Downloader import MyGeneDownloader
from wikidataintegrator import wdi_login, wdi_core, wdi_helpers
from wikidataintegrator.ref_handlers import update_retrieved_if_new
from wikidataintegrator.wdi_fastrun import FastRunContainer
import pprint

core_props = get_default_core_props()

FASTRUN_PROPS = {'Entrez Gene ID', 'strand orientation', 'Saccharomyces Genome Database ID', 'RefSeq RNA ID',
                 'ZFIN Gene ID', 'Ensembl Transcript ID', 'HGNC ID', 'encodes', 'genomic assembly', 'found in taxon',
                 'HomoloGene ID', 'MGI Gene Symbol', 'cytogenetic location', 'Mouse Genome Informatics ID',
                 'FlyBase Gene ID', 'genomic end', 'NCBI Locus tag', 'Rat Genome Database ID', 'Ensembl Gene ID',
                 'instance of', 'chromosome', 'HGNC Gene Symbol', 'Wormbase Gene ID', 'genomic start'}

DAYS = 120
update_retrieved_if_new = partial(update_retrieved_if_new, days=DAYS)

from scheduled_bots.geneprotein import HelperBot, organisms_info, type_of_gene_map, descriptions_by_type
from scheduled_bots.geneprotein.ChromosomeBot import ChromosomeBot
from scheduled_bots.geneprotein.MicrobialChromosomeBot import MicrobialChromosomeBot
from scheduled_bots.geneprotein.HelperBot import make_ref_source, parse_mygene_src_version, source_items

try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

__metadata__ = {
    'name': 'GeneBot',
    'maintainer': 'GSS',
    'tags': ['gene'],
}

# If the source is "entrez", the reference identifier to be used is "Ensembl Gene ID" (P594)
source_ref_id = {
    'ensembl': "Ensembl Gene ID",
    'entrez': 'Entrez Gene ID'
}


class Gene:
    """
    Generic gene class. Subclasses: Human, Mammal, Microbe
    """

    def __init__(self, record, organism_info, login):
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

        self.wd_item_gene = None
        self.label = None
        self.description = None
        self.aliases = None
        self.external_ids = dict()
        self.type_of_gene = None
        self.statements = None
        self.entrez = None
        self.entrez_ref = None
        self.ensembl_ref = None

    def create_description(self):
        if self.type_of_gene is None:
            raise ValueError("must set type_of_gene first")
        self.description = descriptions_by_type[self.type_of_gene].format(self.organism_info['name'])

    def create_label(self):
        self.label = self.record['name']['@value']

    def create_aliases(self):
        if self.label is None:
            self.create_label()
        aliases = []
        if 'symbol' in self.record:
            aliases.append(self.record['symbol']['@value'])
        if 'name' in self.record:
            aliases.append(self.record['name']['@value'])
        if 'NCBI Locus tag' in self.external_ids:
            aliases.append(self.external_ids['NCBI Locus tag'])
        if 'alias' in self.record:
            aliases.extend(self.record['alias']['@value'])
        aliases = set(aliases) - {self.label} - set(descriptions_by_type.keys())
        self.aliases = list(aliases)

    def set_label_desc_aliases(self, wditem):
        wditem.set_label(self.label)
        curr_descr = wditem.get_description()
        if curr_descr == "" or "of the species" in curr_descr or "gene found in" in curr_descr.lower():
            wditem.set_description(self.description)
        wditem.set_aliases(self.aliases)
        return wditem

    def validate_record(self):
        # handled by HelperBot
        # allow for subclasses to add additional checks
        raise NotImplementedError()

    def parse_external_ids(self):
        ############
        # required external IDs
        ############

        entrez_gene = str(self.record['entrezgene']['@value'])
        self.external_ids['Entrez Gene ID'] = entrez_gene
        self.entrez = entrez_gene
        taxid = self.record['taxid']['@value']

        ############
        # optional external IDs
        ############
        # taxid, example gene
        # mouse: 10090, 102466888
        # rat: 10116, 100362233
        # sgd: 559292, 853415
        # fly: 7227, 31303
        # worm: 6239, 174065
        # zfin: 7955, 368434

        # ncbi locus tag
        if 'locus_tag' in self.record:
            self.external_ids['NCBI Locus tag'] = self.record['locus_tag']['@value']

        if 'MGI' in self.record:
            self.external_ids['Mouse Genome Informatics ID'] = self.record['MGI']['@value']
        if 'RGD' in self.record:
            self.external_ids['Rat Genome Database ID'] = self.record['RGD']['@value']
        if 'SGD' in self.record:
            self.external_ids['Saccharomyces Genome Database ID'] = self.record['SGD']['@value']
        if 'FLYBASE' in self.record:
            self.external_ids['FlyBase Gene ID'] = self.record['FLYBASE']['@value']
        if 'WormBase' in self.record:
            self.external_ids['Wormbase Gene ID'] = self.record['WormBase']['@value']
        if 'ZFIN' in self.record:
            self.external_ids['ZFIN Gene ID'] = self.record['ZFIN']['@value']

        if 'HGNC' in self.record:
            self.external_ids['HGNC ID'] = self.record['HGNC']['@value']

        if taxid == 9606 and 'symbol' in self.record and 'HGNC' in self.record:
            # see: https://github.com/stuppie/scheduled-bots/issues/2
            # "and 'HGNC' in record" is required because there is something wrong with mygene
            self.external_ids['HGNC Gene Symbol'] = self.record['symbol']['@value']

        if taxid == 10090 and 'symbol' in self.record:
            self.external_ids['MGI Gene Symbol'] = self.record['symbol']['@value']

        if 'homologene' in self.record:
            self.external_ids['HomoloGene ID'] = str(self.record['homologene']['@value']['id'])

        if 'map_location' in self.record:
            self.external_ids['cytogenetic location'] = self.record['map_location']['@value']

        ############
        # optional external IDs (can have more than one)
        ############
        if 'ensembl' in self.record:
            ensembl_transcript = set(chain(*[x['transcript'] for x in self.record['ensembl']['@value']]))
            self.external_ids['Ensembl Transcript ID'] = ensembl_transcript
            ensembl_gene = [x['gene'] for x in self.record['ensembl']['@value']]
            self.external_ids['Ensembl Gene ID'] = ensembl_gene

        # RefSeq RNA ID
        if 'refseq' in self.record and 'rna' in self.record['refseq']['@value']:
            self.external_ids['RefSeq RNA ID'] = self.record['refseq']['@value']['rna']

    def create_ref_sources(self):
        # create an entrez ref and ensembl ref (optional)
        self.entrez_ref = make_ref_source(self.record['entrezgene']['@source'], PROPS['Entrez Gene ID'],
                                          self.external_ids['Entrez Gene ID'], login=self.login)
        if 'Ensembl Gene ID' in self.external_ids:
            if len(self.external_ids['Ensembl Gene ID']) != 1:
                raise ValueError("more than one ensembl gene ID: {}".format(self.record['entrezgene']))
            ensembl_gene_id = list(self.external_ids['Ensembl Gene ID'])[0]
            self.ensembl_ref = make_ref_source(self.record['ensembl']['@source'], PROPS['Ensembl Gene ID'],
                                               ensembl_gene_id, login=self.login)

    def create_statements(self):
        """
        create statements common to all genes
        """
        s = []
        if not self.entrez_ref:
            self.create_ref_sources()

        ############
        # ID statements (required)
        ############
        s.append(wdi_core.WDString(self.external_ids['Entrez Gene ID'], PROPS['Entrez Gene ID'],
                                   references=[self.entrez_ref]))

        # optional ID statements
        if self.ensembl_ref:
            for ensembl_gene_id in self.external_ids['Ensembl Gene ID']:
                s.append(wdi_core.WDString(ensembl_gene_id, PROPS['Ensembl Gene ID'], references=[self.ensembl_ref]))

            if 'Ensembl Transcript ID' in self.external_ids:
                for id in self.external_ids['Ensembl Transcript ID']:
                    s.append(wdi_core.WDString(id, PROPS['Ensembl Transcript ID'], references=[self.ensembl_ref]))

        key = 'RefSeq RNA ID'
        if key in self.external_ids:
            for id in self.external_ids[key]:
                s.append(wdi_core.WDString(id, PROPS[key], references=[self.entrez_ref]))

        for key in ['NCBI Locus tag', 'Saccharomyces Genome Database ID', 'Mouse Genome Informatics ID',
                    'MGI Gene Symbol', 'HomoloGene ID', 'Rat Genome Database ID', 'FlyBase Gene ID',
                    'Wormbase Gene ID', 'ZFIN Gene ID', 'cytogenetic location']:
            if key in self.external_ids:
                s.append(wdi_core.WDString(self.external_ids[key], PROPS[key], references=[self.entrez_ref]))

        ############
        # Gene statements
        ############
        # if there is an ensembl ID, this comes from ensembl, otherwise, entrez
        gene_ref = self.ensembl_ref if self.ensembl_ref is not None else self.entrez_ref

        # instance of gene, ncRNA.. etc
        type_of_gene = self.record['type_of_gene']['@value']
        assert type_of_gene in type_of_gene_map, "unknown type of gene: {}".format(type_of_gene)
        self.type_of_gene = type_of_gene
        # "protein-coding gene" will be instance of "gene"
        s.append(wdi_core.WDItemID(type_of_gene_map[type_of_gene], PROPS['instance of'], references=[gene_ref]))

        if type_of_gene not in {'protein-coding', 'pseudo', 'other', 'unknown'}:
            # make sure we add instance of "gene" as well
            s.append(wdi_core.WDItemID("Q7187", PROPS['instance of'], references=[gene_ref]))

        # found in taxon
        s.append(wdi_core.WDItemID(self.organism_info['wdid'], PROPS['found in taxon'], references=[gene_ref]))

        return s

    def create_item(self, fast_run=True, write=True):
        self.parse_external_ids()
        self.statements = self.create_statements()
        # remove subclass of gene statements
        # s = wdi_core.WDItemID("Q7187", "P279")
        # setattr(s, 'remove', '')
        self.create_label()
        self.create_description()
        self.create_aliases()

        self.fast_run_base_filter = {PROPS['Entrez Gene ID']: '',
                                     PROPS['found in taxon']: self.organism_info['wdid']}

        self.wd_item_gene = wdi_core.WDItemEngine(data=self.statements,
                                                  append_value=[PROPS['instance of']],
                                                  fast_run=fast_run, fast_run_base_filter=self.fast_run_base_filter,
                                                  fast_run_use_refs=True, ref_handler=update_retrieved_if_new,
                                                  global_ref_mode="CUSTOM",
                                                  core_props=core_props)

        self.wd_item_gene = self.set_label_desc_aliases(self.wd_item_gene)
        self.status = wdi_helpers.try_write(self.wd_item_gene, self.external_ids['Entrez Gene ID'],
                                            PROPS['Entrez Gene ID'],
                                            self.login, write=write)


class ChromosomalGene(Gene):
    """
    yeast, mouse, rat, worm, fly, zebrafish
    """

    def __init__(self, record, organism_info, chr_num_wdid, login):
        """
        :param chr_num_wdid: mapping of chr number (str) to wdid
        """
        super().__init__(record, organism_info, login)
        self.chr_num_wdid = chr_num_wdid

    def create_label(self):
        self.label = self.record['symbol']['@value']

    def create_statements(self):

        # create generic gene statements
        s = super().create_statements()

        # add on gene position statements
        if 'genomic_pos' in self.record:
            ss = self.create_gp_statements_chr()
            if ss:
                s.extend(ss)

        return s

    def parse_external_ids(self):
        super().parse_external_ids()
        if 'Ensembl Gene ID' in self.external_ids:
            # figure out which to use as reference
            genomic_pos_values = self.record['genomic_pos']['@value']
            genomic_pos_values = [x for x in genomic_pos_values if x['chr'] in self.chr_num_wdid]
            if len(genomic_pos_values) == 1:
                genomic_pos_value = genomic_pos_values[0]
                if 'ensemblgene' in genomic_pos_value:
                    self.external_ids['Reference Ensembl Gene ID'] = genomic_pos_value['ensemblgene']

    def create_ref_sources(self):
        # create an entrez ref and ensembl ref (optional)
        self.entrez_ref = make_ref_source(self.record['entrezgene']['@source'], PROPS['Entrez Gene ID'],
                                          self.external_ids['Entrez Gene ID'], login=self.login)
        if 'Reference Ensembl Gene ID' in self.external_ids:
            self.ensembl_ref = make_ref_source(self.record['ensembl']['@source'], PROPS['Ensembl Gene ID'],
                                               self.external_ids['Reference Ensembl Gene ID'], login=self.login)
        elif 'Ensembl Gene ID' in self.external_ids:
            if len(self.external_ids['Ensembl Gene ID']) == 1:
                self.ensembl_ref = make_ref_source(self.record['ensembl']['@source'], PROPS['Ensembl Gene ID'],
                                               list(self.external_ids['Ensembl Gene ID'])[0], login=self.login)

    def create_item(self, fast_run=True, write=True):
        self.parse_external_ids()
        self.create_ref_sources()
        return super().create_item(fast_run, write)

    def create_gp_statements_chr(self):
        """
        Create genomic_pos start stop orientation on a chromosome
        :return:
        """
        if not self.entrez_ref:
            self.create_ref_sources()

        genomic_pos_values = self.record['genomic_pos']['@value']
        genomic_pos_source = self.record['genomic_pos']['@source']
        if genomic_pos_source['id'] == "entrez":
            genomic_pos_ref = self.entrez_ref
        elif genomic_pos_source['id'] == "ensembl":
            genomic_pos_ref = self.ensembl_ref
        else:
            raise ValueError()
        if not genomic_pos_ref:
            return None
        all_chr = set([self.chr_num_wdid[x['chr']] for x in genomic_pos_values])
        all_strand = set(['Q22809680' if x['strand'] == 1 else 'Q22809711' for x in genomic_pos_values])

        s = []
        for genomic_pos_value in genomic_pos_values:
            # create qualifier for start/stop/orientation
            chrom_wdid = self.chr_num_wdid[genomic_pos_value['chr']]
            qualifiers = [wdi_core.WDItemID(chrom_wdid, PROPS['chromosome'], is_qualifier=True)]

            # genomic start and end
            s.append(wdi_core.WDString(str(int(genomic_pos_value['start'])), PROPS['genomic start'],
                                       references=[genomic_pos_ref], qualifiers=qualifiers))
            s.append(wdi_core.WDString(str(int(genomic_pos_value['end'])), PROPS['genomic end'],
                                       references=[genomic_pos_ref], qualifiers=qualifiers))

        for chr in all_chr:
            s.append(wdi_core.WDItemID(chr, PROPS['chromosome'], references=[genomic_pos_ref]))

        if len(all_strand) == 1:
            # todo: not sure what to do if you have both orientations on the same chr
            strand_orientation = list(all_strand)[0]
            s.append(wdi_core.WDItemID(strand_orientation, PROPS['strand orientation'], references=[genomic_pos_ref]))

        return s


class MicrobeGene(Gene):
    """
    Microbes

    """

    def __init__(self, record, organism_info, refseq_qid_chrom, login):
        super().__init__(record, organism_info, login)
        self.refseq_qid_chrom = refseq_qid_chrom

    def create_label(self):
        self.label = self.record['name']['@value'] + " " + self.record['locus_tag']['@value']

    def create_description(self):
        if self.organism_info['type']:
            self.description = '{} gene found in {}'.format(self.organism_info['type'], self.organism_info['name'])
        else:
            self.description = 'Gene found in {}'.format(self.organism_info['name'])

    def validate_record(self):
        pass

    def create_statements(self):
        # create generic gene statements
        s = super().create_statements()

        # add on gene position statements
        s.extend(self.create_gp_statements())

        return s

    def create_gp_statements(self):
        """
        Create genomic_pos start stop orientation plus chromosome qualifiers
        :return:
        """
        genomic_pos_value = self.record['genomic_pos']['@value'][0]
        genomic_pos_source = self.record['genomic_pos']['@source']
        genomic_pos_id_prop = source_ref_id[genomic_pos_source['id']]
        assert isinstance(self.external_ids[genomic_pos_id_prop], str)
        external_id = self.external_ids[genomic_pos_id_prop]

        genomic_pos_ref = make_ref_source(genomic_pos_source, PROPS[genomic_pos_id_prop], external_id, login=self.login)

        s = []

        # create qualifier for chromosome (which has the refseq ID on it)
        chr_refseq = genomic_pos_value['chr']
        chr_qid = self.refseq_qid_chrom[chr_refseq]
        qualifiers = [wdi_core.WDItemID(value=chr_qid, prop_nr=PROPS['chromosome'], is_qualifier=True)]

        # strand orientation
        strand_orientation = 'Q22809680' if genomic_pos_value['strand'] == 1 else 'Q22809711'
        s.append(wdi_core.WDItemID(strand_orientation, PROPS['strand orientation'],
                                   references=[genomic_pos_ref], qualifiers=qualifiers))
        # genomic start and end
        s.append(wdi_core.WDString(str(int(genomic_pos_value['start'])), PROPS['genomic start'],
                                   references=[genomic_pos_ref], qualifiers=qualifiers))
        s.append(wdi_core.WDString(str(int(genomic_pos_value['end'])), PROPS['genomic end'],
                                   references=[genomic_pos_ref], qualifiers=qualifiers))

        return s


class HumanGene(ChromosomalGene):
    def create_statements(self):
        # create gene statements
        s = Gene.create_statements(self)
        entrez_ref = make_ref_source(self.record['entrezgene']['@source'], PROPS['Entrez Gene ID'],
                                     self.external_ids['Entrez Gene ID'], login=self.login)

        # add on human specific gene statements
        for key in ['HGNC ID', 'HGNC Gene Symbol']:
            if key in self.external_ids:
                s.append(wdi_core.WDString(self.external_ids[key], PROPS[key], references=[entrez_ref]))

        # add on gene position statements
        if 'genomic_pos' in self.record:
            ss = self.do_gp_human()
            if ss:
                s.extend(ss)

        return s

    def validate_record(self):
        assert 'locus_tag' in self.record
        assert 'HGNC' in self.record
        assert 'symbol' in self.record
        assert 'ensembl' in self.record and 'transcript' in self.record['ensembl']
        assert 'refseq' in self.record and 'rna' in self.record['ensembl']
        assert 'alias' in self.record

    def do_gp_human(self):
        """
        create genomic pos, chr, strand statements for human
        includes genomic assembly

        genes that are on an unlocalized scaffold will have no genomic position statements
        example: https://mygene.info/v3/gene/102724770
        https://www.wikidata.org/wiki/Q20970159
        :return:
        """
        if not self.entrez_ref:
            self.create_ref_sources()

        genomic_pos_values = self.record['genomic_pos']['@value']
        genomic_pos_source = self.record['genomic_pos']['@source']
        genomic_pos_id_prop = source_ref_id[genomic_pos_source['id']]
        if genomic_pos_source['id'] == "entrez":
            genomic_pos_ref = self.entrez_ref
        elif genomic_pos_source['id'] == "ensembl":
            genomic_pos_ref = self.ensembl_ref
        else:
            raise ValueError()
        if not genomic_pos_ref:
            return None
        assembly_hg38 = wdi_core.WDItemID("Q20966585", PROPS['genomic assembly'], is_qualifier=True)

        for x in genomic_pos_values:
            x['assembly'] = 'hg38'

        do_hg19 = False
        if 'genomic_pos_hg19' in self.record:
            do_hg19 = True
            genomic_pos_value_hg19 = self.record['genomic_pos_hg19']['@value']
            genomic_pos_source_hg19 = self.record['genomic_pos_hg19']['@source']
            genomic_pos_id_prop_hg19 = source_ref_id[genomic_pos_source_hg19['id']]
            assembly_hg19 = wdi_core.WDItemID("Q21067546", PROPS['genomic assembly'], is_qualifier=True)
            # combine all together
            for x in genomic_pos_value_hg19:
                x['assembly'] = 'hg19'
            genomic_pos_values.extend(genomic_pos_value_hg19)

        # remove those where we don't know the chromosome
        genomic_pos_values = [x for x in genomic_pos_values if x['chr'] in self.chr_num_wdid]
        # print(len(genomic_pos_values))

        all_chr = set([self.chr_num_wdid[x['chr']] for x in genomic_pos_values])
        all_strand = set(['Q22809680' if x['strand'] == 1 else 'Q22809711' for x in genomic_pos_values])

        s = []
        for genomic_pos_value in genomic_pos_values:

            # create qualifiers (chromosome and assembly)
            chrom_wdid = self.chr_num_wdid[genomic_pos_value['chr']]
            qualifiers = [wdi_core.WDItemID(chrom_wdid, PROPS['chromosome'], is_qualifier=True)]
            if genomic_pos_value['assembly'] == 'hg38':
                qualifiers.append(assembly_hg38)
                ref = genomic_pos_ref
            elif genomic_pos_value['assembly'] == 'hg19':
                qualifiers.append(assembly_hg19)
                ref = genomic_pos_ref

            # genomic start and end
            s.append(wdi_core.WDString(str(int(genomic_pos_value['start'])), PROPS['genomic start'],
                                       references=[ref], qualifiers=qualifiers))
            s.append(wdi_core.WDString(str(int(genomic_pos_value['end'])), PROPS['genomic end'],
                                       references=[ref], qualifiers=qualifiers))

        # strand orientations
        # if the same for all, only put one statement
        if len(all_strand) == 1 and do_hg19:
            strand_orientation = list(all_strand)[0]
            s.append(wdi_core.WDItemID(strand_orientation, PROPS['strand orientation'],
                                       references=[genomic_pos_ref], qualifiers=[assembly_hg38, assembly_hg19]))
        elif len(all_strand) == 1 and not do_hg19:
            strand_orientation = list(all_strand)[0]
            s.append(wdi_core.WDItemID(strand_orientation, PROPS['strand orientation'],
                                       references=[genomic_pos_ref], qualifiers=[assembly_hg38]))

        # chromosome
        # if the same for all, only put one statement
        if do_hg19 and len(all_chr) == 1:
            chrom_wdid = list(all_chr)[0]
            s.append(wdi_core.WDItemID(chrom_wdid, PROPS['chromosome'],
                                       references=[genomic_pos_ref], qualifiers=[assembly_hg38, assembly_hg19]))
        elif len(all_chr) == 1 and not do_hg19:
            chrom_wdid = list(all_chr)[0]
            s.append(wdi_core.WDItemID(chrom_wdid, PROPS['chromosome'],
                                       references=[genomic_pos_ref], qualifiers=[assembly_hg38]))

        # print(s)
        return s


class GeneBot:
    """
    Generic genebot class
    """
    GENE_CLASS = Gene
    item = None
    failed = []  # list of entrez ids for those that failed

    def __init__(self, organism_info, login):
        self.login = login
        self.organism_info = organism_info

    def run(self, records, total=None, fast_run=True, write=True):
        # this shouldn't ever actually get used now
        raise ValueError()
        records = self.filter(records)
        for record in tqdm(records, mininterval=2, total=total):
            gene = self.GENE_CLASS(record, self.organism_info, self.login)
            try:
                gene.create_item(fast_run=fast_run, write=write)
            except Exception as e:
                exc_info = sys.exc_info()
                traceback.print_exception(*exc_info)
                msg = wdi_helpers.format_msg(gene.external_ids['Entrez Gene ID'], PROPS['Entrez Gene ID'], None,
                                             str(e), msg_type=type(e))
                wdi_core.WDItemEngine.log("ERROR", msg)
                gene.status = msg

            if gene.status is not True:
                self.failed.append(gene.entrez)

    def filter(self, records):
        """
        This is used to selectively skip certain records based on conditions within the record or to specifically
        alter certain fields before sending to the Bot
        """
        # If we are processing zebrafish records, skip the record if it doesn't have a zfin ID
        for record in records:
            if record['taxid']['@value'] == 7955 and 'ZFIN' not in record:
                continue
            else:
                yield record

    def cleanup(self, releases, last_updated):
        """

        :param releases:
        :param last_updated:
        :param failed: list of entrez ids to skip
        :return:
        """
        print(self.failed)
        entrez_qid = wdi_helpers.id_mapper('P351', ((PROPS['found in taxon'], self.organism_info['wdid']),))
        print(len(entrez_qid))
        entrez_qid = {entrez: qid for entrez, qid in entrez_qid.items() if entrez not in self.failed}
        print(len(entrez_qid))
        filter = {PROPS['Entrez Gene ID']: '', PROPS['found in taxon']: self.organism_info['wdid']}
        frc = FastRunContainer(wdi_core.WDBaseDataType, wdi_core.WDItemEngine, base_filter=filter, use_refs=True)
        frc.clear()
        props = [PROPS[x] for x in FASTRUN_PROPS]
        for qid in tqdm(entrez_qid.values()):
            remove_deprecated_statements(qid, frc, releases, last_updated, props, self.login)


class ChromosomalGeneBot(GeneBot):
    GENE_CLASS = ChromosomalGene

    def __init__(self, organism_info, chr_num_wdid, login):
        super().__init__(organism_info, login)
        self.chr_num_wdid = chr_num_wdid

    def run(self, records, total=None, fast_run=True, write=True):
        records = self.filter(records)
        for record in tqdm(records, mininterval=2, total=total):
            # print(record['entrezgene'])
            gene = self.GENE_CLASS(record, self.organism_info, self.chr_num_wdid, self.login)
            try:
                gene.create_item(fast_run=fast_run, write=write)
            except Exception as e:
                exc_info = sys.exc_info()
                traceback.print_exception(*exc_info)
                msg = wdi_helpers.format_msg(gene.external_ids['Entrez Gene ID'], PROPS['Entrez Gene ID'], None,
                                             str(e), msg_type=type(e))
                wdi_core.WDItemEngine.log("ERROR", msg)
                gene.status = msg
            if gene.status is not True:
                self.failed.append(gene.entrez)


class HumanGeneBot(ChromosomalGeneBot):
    GENE_CLASS = HumanGene


class MicrobeGeneBot(ChromosomalGeneBot):
    GENE_CLASS = MicrobeGene


def remove_deprecated_statements(qid, frc, releases, last_updated, props, login):
    """
    :param qid: qid of item
    :param frc: a fastrun container
    :param releases: list of releases to remove (a statement that has a reference that is stated in one of these
            releases will be removed)
    :param last_updated: looks like {'Q20641742': datetime.date(2017,5,6)}. a statement that has a reference that is
            stated in Q20641742 (entrez) and was retrieved more than DAYS before 2017-5-6 will be removed
    :param props: look at these props
    :param login:
    :return:
    """
    for prop in props:
        frc.write_required([wdi_core.WDString("fake value", prop)])
    orig_statements = frc.reconstruct_statements(qid)
    releases = set(int(r[1:]) for r in releases)

    s_dep = []
    for s in orig_statements:
        if any(any(x.get_prop_nr() == 'P248' and x.get_value() in releases for x in r) for r in s.get_references()):
            setattr(s, 'remove', '')
            s_dep.append(s)
        else:
            for r in s.get_references():
                dbs = [x.get_value() for x in r if x.get_value() in last_updated]
                if dbs:
                    db = dbs[0]
                    if any(x.get_prop_nr() == 'P813' and last_updated[db] - x.get_value() > DAYS for x in r):
                        setattr(s, 'remove', '')
                        s_dep.append(s)
    if s_dep:
        print("-----")
        print(qid)
        print(len(s_dep))
        print([(x.get_prop_nr(), x.value) for x in s_dep])
        print([(x.get_references()[0]) for x in s_dep])
        wd_item = wdi_core.WDItemEngine(wd_item_id=qid, data=s_dep, fast_run=False)
        wdi_helpers.try_write(wd_item, '', '', login, edit_summary="remove deprecated statements")


def main(taxid, metadata, log_dir="./logs", run_id=None, fast_run=True, write=True, entrez=None):
    """
    Main function for creating/updating genes

    :param taxid: taxon to use (ncbi tax id)
    :type taxid: str
    :param metadata: looks like: {"ensembl" : 84, "cpdb" : 31, "netaffy" : "na35", "ucsc" : "20160620", .. }
    :type metadata: dict
    :param log_dir: dir to store logs
    :type log_dir: str
    :param fast_run: use fast run mode
    :type fast_run: bool
    :param write: actually perform write
    :type write: bool
    :param entrez: Only run this one gene
    :type entrez: int
    :return: None
    """

    # make sure the organism is found in wikidata
    taxid = int(taxid)
    organism_wdid = wdi_helpers.prop2qid("P685", str(taxid))
    if not organism_wdid:
        print("organism {} not found in wikidata".format(taxid))
        return None

    # login
    login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS)
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []
        wdi_core.WDItemEngine.logger.handlers = []

    run_id = run_id if run_id is not None else datetime.now().strftime('%Y%m%d_%H:%M')
    log_name = '{}-{}.log'.format(__metadata__['name'], run_id)
    __metadata__['taxid'] = taxid
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, logger_name='WD_logger', log_name=log_name,
                                        header=json.dumps(__metadata__))

    # get organism metadata (name, organism type, wdid)
    # TODO: this can be pulled from wd
    if taxid in organisms_info and organisms_info[taxid]['type'] != "microbial":
        validate_type = 'eukaryotic'
        organism_info = organisms_info[taxid]
        # make sure all chromosome items are found in wikidata
        cb = ChromosomeBot()
        chr_num_wdid = cb.get_or_create(organism_info, login=login)
        chr_num_wdid = {k.upper(): v for k, v in chr_num_wdid.items()}
        if int(organism_info['taxid']) == 9606:
            bot = HumanGeneBot(organism_info, chr_num_wdid, login)
        else:
            bot = ChromosomalGeneBot(organism_info, chr_num_wdid, login)
    else:
        # check if its one of the reference microbial genomes
        # raises valueerror if not...
        organism_info = mcb.get_organism_info(taxid)
        refseq_qid_chrom = mcb.get_or_create_chromosomes(taxid, login)
        print(organism_info)
        bot = MicrobeGeneBot(organism_info, refseq_qid_chrom, login)
        validate_type = "microbial"

    # Get handle to mygene records
    mgd = MyGeneDownloader()
    if entrez:
        doc, total = mgd.get_mg_gene(entrez)
        docs = iter([doc])
    else:
        doc_filter = lambda x: (x.get("type_of_gene") != "biological-region") and ("entrezgene" in x)
        docs, total = mgd.get_mg_cursor(taxid, doc_filter)
    print("total number of records: {}".format(total))
    # the scroll_id/cursor times out from mygene if we iterate. So.... get the whole thing now
    docs = list(docs)
    docs = HelperBot.validate_docs(docs, validate_type, PROPS['Entrez Gene ID'])
    records = HelperBot.tag_mygene_docs(docs, metadata)

    bot.run(records, total=total, fast_run=fast_run, write=write)
    for frc in wdi_core.WDItemEngine.fast_run_store:
        frc.clear()
    print("done updating, waiting 10 min")
    time.sleep(10 * 60)
    releases = dict()
    releases_to_remove = set()
    last_updated = dict()
    metadata = {k: v for k, v in metadata.items() if k in {'uniprot', 'ensembl', 'entrez'}}
    for k, v in parse_mygene_src_version(metadata).items():
        if "release" in v:
            if k not in releases:
                releases[k] = wdi_helpers.id_mapper('P393', (('P629', source_items[k]),))
            to_remove = set(releases[k].values())
            to_remove.discard(releases[k][v['release']])
            releases_to_remove.update(to_remove)
            print(
                "{}: Removing releases: {}, keeping release: {}".format(k, ", ".join(set(releases[k]) - {v['release']}),
                                                                        v['release']))
        else:
            last_updated[source_items[k]] = datetime.strptime(v["timestamp"], "%Y%m%d")
    print(last_updated)
    bot.cleanup(releases_to_remove, last_updated)


if __name__ == "__main__":
    """
    Data to be used is retrieved from mygene.info
    """
    parser = argparse.ArgumentParser(description='run wikidata gene bot')
    parser.add_argument('--log-dir', help='directory to store logs', type=str)
    parser.add_argument('--dummy', help='do not actually do write', action='store_true')
    parser.add_argument('--taxon',
                        help="only run using this taxon (ncbi tax id). or 'microbe' for all microbes. comma separated",
                        type=str, required=True)
    parser.add_argument('--fastrun', dest='fastrun', action='store_true')
    parser.add_argument('--no-fastrun', dest='fastrun', action='store_false')
    parser.add_argument('--entrez', help="Run only this one gene")
    parser.set_defaults(fastrun=True)
    args = parser.parse_args()
    log_dir = args.log_dir if args.log_dir else "./logs"
    run_id = datetime.now().strftime('%Y%m%d_%H:%M')
    __metadata__['run_id'] = run_id
    taxon = args.taxon
    fast_run = args.fastrun
    mcb = MicrobialChromosomeBot()

    # get metadata about sources
    mgd = MyGeneDownloader()
    pprint.pprint(mgd.get_metadata())
    metadata = mgd.get_metadata()['src_version']

    if args.entrez:
        main(taxon, metadata, run_id=run_id, log_dir=log_dir, fast_run=fast_run,
             write=not args.dummy, entrez=args.entrez)
        sys.exit(0)

    if "microbe" in taxon:
        microbe_taxa = mcb.get_all_taxids()
        taxon = taxon.replace("microbe", ','.join(map(str, microbe_taxa)))

    for taxon1 in taxon.split(","):
        try:
            main(taxon1, metadata, run_id=run_id, log_dir=log_dir, fast_run=fast_run, write=not args.dummy)
        except Exception as e:
            # if one taxon fails, still try to run the others
            traceback.print_exc()
        # done with this run, clear fast run container to save on RAM
        wdi_core.WDItemEngine.fast_run_store = []
        wdi_core.WDItemEngine.fast_run_container = None
