from .Gene import Gene
from scheduled_bots import PROPS
from wikidataintegrator import wdi_core

class ChromosomalGene(Gene):
    """
    yeast, mouse, rat, worm, fly, zebrafish
    """

    def __init__(self, record, organism_info, chr_num_wdid, ref_factory):
        """
        :param chr_num_wdid: mapping of chr number (str) to wdid
        """
        super().__init__(record, organism_info, ref_factory)
        self.chr_num_wdid = chr_num_wdid

    def create_label(self):
        self.label = self.record['symbol']['@value']

    def create_statements(self, refseq=False, deprecated_entrez=False):

        # create generic gene statements
        s = super().create_statements(refseq=refseq, deprecated_entrez=deprecated_entrez)

        # add on gene position statements
        if 'genomic_pos' in self.record:
            ss = self.create_gp_statements_chr(refseq=refseq)
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
        super().create_ref_sources()

        if 'Reference Ensembl Gene ID' in self.external_ids and self.external_ids['Reference Ensembl Gene ID']:
            self.ensembl_ref = self.ref_factory.get_reference(self.record['ensembl']['@source'], PROPS['Ensembl Gene ID'],
                                               self.external_ids['Reference Ensembl Gene ID'])

    def create_gp_statements_chr(self, refseq=False):
        """
        Create genomic_pos start stop orientation on a chromosome
        :return:
        """

        genomic_pos_values = self.record['genomic_pos']['@value']
        genomic_pos_source = self.record['genomic_pos']['@source']

        if self.entrez_ref:
            genomic_pos_refs = [self.entrez_ref]
        elif self.ensembl_ref:
            genomic_pos_refs = [self.ensembl_ref]
        elif self.refseq_ref:
            genomic_pos_refs = [self.refseq_ref]
        else:
            print("No genomic position ref")
            genomic_pos_refs = []

        if refseq and self.refseq_ref:
            genomic_pos_refs = [self.refseq_ref]

        all_chr = set([self.chr_num_wdid[x['chr']] for x in genomic_pos_values])
        all_strand = set(['Q22809680' if x['strand'] == 1 else 'Q22809711' for x in genomic_pos_values])

        s = []
        for genomic_pos_value in genomic_pos_values:
            # create qualifier for start/stop/orientation
            chrom_wdid = self.chr_num_wdid[genomic_pos_value['chr']]
            qualifiers = [wdi_core.WDItemID(chrom_wdid, PROPS['chromosome'], is_qualifier=True)]

            # genomic start and end
            s.append(wdi_core.WDString(str(int(genomic_pos_value['start'])), PROPS['genomic start'],
                                       references=genomic_pos_refs, qualifiers=qualifiers))
            s.append(wdi_core.WDString(str(int(genomic_pos_value['end'])), PROPS['genomic end'],
                                       references=genomic_pos_refs, qualifiers=qualifiers))

        for chr in all_chr:
            s.append(wdi_core.WDItemID(chr, PROPS['chromosome'], references=genomic_pos_refs))

        if len(all_strand) == 1:
            # todo: not sure what to do if you have both orientations on the same chr
            strand_orientation = list(all_strand)[0]
            s.append(wdi_core.WDItemID(strand_orientation, PROPS['strand orientation'], references=genomic_pos_refs))

        return s