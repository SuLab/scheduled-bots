import subprocess
from datetime import datetime

import pandas as pd

from scheduled_bots import get_default_core_props
from scheduled_bots.geneprotein import PROPS
from wikidataintegrator import wdi_core, wdi_helpers
from wikidataintegrator.wdi_helpers import prop2qid

core_props = get_default_core_props()


class MicrobialChromosomeBot:
    chr_type_map = {'chromosome': 'Q37748',
                    'mitochondrion': 'Q18694495',
                    'chloroplast': 'Q22329079',
                    'plasmid': 'Q172778',
                    'circular': 'Q5121654'}

    def __init__(self):
        self.df = pd.DataFrame()

    def get_microbial_ref_genome_table(self):
        """
        # Download and parse Microbial Reference and representative genomes table
        # https://www.ncbi.nlm.nih.gov/genome/browse/reference/
        # but oh wait, it has no useful fields that we need (taxid, accession, chromosome)
        url = "https://www.ncbi.nlm.nih.gov/genomes/Genome2BE/genome2srv.cgi?action=refgenomes&download=on&type=reference"
        df_ref = pd.read_csv(url, sep="\t", dtype=object, header=0)
        """

        # get the assembly accessions of the reference genomes
        url = "ftp://ftp.ncbi.nlm.nih.gov/genomes/refseq/bacteria/assembly_summary.txt"
        subprocess.check_output(['wget', '-N', url])
        assdf = pd.read_csv("assembly_summary.txt", sep="\t", dtype=object, header=0, skiprows=1)
        assdf = assdf.query("refseq_category == 'reference genome'")
        accessions = set(assdf.gbrs_paired_asm)
        # adding Chlamydia muridarum str. Nigg
        accessions.add("GCA_000006685.1")
        accessions.add("GCA_000012685.1")

        # Download prokaryotes genome table
        # but oh wait, it has no ref genome column
        url = "ftp://ftp.ncbi.nlm.nih.gov/genomes/GENOME_REPORTS/prokaryotes.txt"
        subprocess.check_output(['wget', '-N', url])
        df = pd.read_csv("prokaryotes.txt", sep="\t", dtype=object, header=0)
        df = df[df['Assembly Accession'].isin(accessions)]
        df = df.rename(columns={df.columns[0]: df.columns[0][1:]})
        # columns = ['Organism/Name', 'TaxID', 'BioProject Accession', 'BioProject ID', 'Group', 'SubGroup', 'Size (Mb)',
        #            'GC%', 'Replicons', 'WGS', 'Scaffolds', 'Genes', 'Proteins', 'Release Date',
        #            'Modify Date', 'Status', 'Center', 'BioSample Accession', 'Assembly Accession', 'Reference',
        #            'FTP Path', 'Pubmed ID', 'Strain']

        print("Found {} reference genomes".format(len(df)))

        assert len(set(df.TaxID)) == len(df)
        assert 110 < len(df) < 140

        self.df = df

    def get_chromosome_info(self, taxid):
        # replicons column looks like:
        # 'chromosome circular:NC_003062.2/AE007869.2; chromosome linear:NC_003063.2/AE007870.2; plasmid At:NC_003064.2/AE007872.2; plasmid Ti:NC_003065.3/AE007871.2'
        replicons = self.df.loc[self.df.TaxID == taxid, 'Replicons'].values[0]
        chroms = [{'name': x.split(":")[0].strip(),
                   'refseq': x.split(":")[1].split("/")[0].strip()} for x in replicons.split(";")]
        return chroms

    def get_or_create_chromosomes(self, taxid, login=None):
        # main function to use to get or create all of the chromosomes for a bacterial organism
        # returns dict with key = refseq ID, value = qid for chromosome item
        if self.df.empty:
            self.get_microbial_ref_genome_table()
        df = self.df

        taxid = str(taxid)
        entry = df[df.TaxID == taxid].to_dict("records")[0]
        organism_name = entry['Organism/Name']
        organism_qid = prop2qid(PROPS['NCBI Taxonomy ID'], taxid)

        chroms = self.get_chromosome_info(taxid)
        chr_map = dict()
        chr_name_type = {'chromosome circular': 'circular',
                         'chromosome linear': 'chromosome',
                         'chromosome': 'chromosome'}
        for chrom in chroms:
            chrom_name = chrom['name'].lower()
            genome_id = chrom['refseq']
            if chrom_name in chr_name_type:
                chr_type = chr_name_type[chrom_name]
            elif "plasmid" in chrom_name:
                chr_type = 'plasmid'
            else:
                raise ValueError("unknown chromosome type: {}".format(chrom['name']))
            qid = self.create_chrom(organism_name, organism_qid, chrom_name, genome_id, chr_type, login=login)
            chr_map[chrom['refseq']] = qid

        return chr_map

    def create_chrom(self, organism_name, organism_qid, chrom_name, genome_id, chr_type, login):

        def make_ref(retrieved, genome_id):
            """
            Create reference statement for chromosomes
            :param retrieved: datetime
            :type retrieved: datetime
            :param genome_id: refseq genome id
            :type genome_id: str
            :return:
            """
            refs = [
                wdi_core.WDItemID(value='Q20641742', prop_nr='P248', is_reference=True),  # stated in ncbi gene
                wdi_core.WDString(value=genome_id, prop_nr='P2249', is_reference=True),  # Link to Refseq Genome ID
                wdi_core.WDTime(retrieved.strftime('+%Y-%m-%dT00:00:00Z'), prop_nr='P813', is_reference=True)
            ]
            return refs

        item_name = '{} {}'.format(organism_name, chrom_name)
        item_description = 'bacterial {}'.format(chr_type)
        print(genome_id)

        retrieved = datetime.now()
        reference = make_ref(retrieved, genome_id)

        # instance of chr_type
        chr_type = chr_type.lower()
        if chr_type not in self.chr_type_map:
            raise ValueError("unknown chromosome type: {}".format(chr_type))
        statements = [wdi_core.WDItemID(value=self.chr_type_map[chr_type], prop_nr='P31', references=[reference])]
        # found in taxon
        statements.append(wdi_core.WDItemID(value=organism_qid, prop_nr='P703', references=[reference]))
        # genome id
        statements.append(wdi_core.WDString(value=genome_id, prop_nr='P2249', references=[reference]))

        wd_item = wdi_core.WDItemEngine(data=statements,
                                        append_value=['P31'], fast_run=True,
                                        fast_run_base_filter={'P703': organism_qid, 'P2249': ''},
                                        core_props=core_props)
        if wd_item.wd_item_id:
            return wd_item.wd_item_id
        if login is None:
            raise ValueError("Login is required to create item")
        wd_item.set_label(item_name)
        wd_item.set_description(item_description, lang='en')
        wdi_helpers.try_write(wd_item, genome_id, 'P2249', login)
        return wd_item.wd_item_id

    def get_all_taxids(self):
        if self.df.empty:
            self.get_microbial_ref_genome_table()
        return set(self.df.TaxID)

    def get_organism_info(self, taxid):
        taxid = str(taxid)
        if taxid not in self.get_all_taxids():
            raise ValueError("taxid {} not found in microbe ref genomes".format(taxid))
        entry = self.df[self.df.TaxID == taxid].to_dict("records")[0]
        qid = prop2qid(PROPS['NCBI Taxonomy ID'], taxid)
        return {'name': entry['Organism/Name'],
                'type': "microbial",
                'wdid': qid,
                'qid': qid,
                'taxid': taxid}
