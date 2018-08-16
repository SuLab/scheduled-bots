from .ChromosomalGene import ChromosomalGene
from scheduled_bots import PROPS
from wikidataintegrator import wdi_core

# If the source is "entrez", the reference identifier to be used is "Ensembl Gene ID" (P594)
source_ref_id = {
    'ensembl': "Ensembl Gene ID",
    'entrez': 'Entrez Gene ID'
}

class HumanGene(ChromosomalGene):

    '''
    def create_statements(self):
        # create gene statements
        #s = Gene.create_statements(self)
        s = super().create_statements()
        #entrez_ref = make_ref_source(self.record['entrezgene']['@source'], PROPS['Entrez Gene ID'],
        #                             self.external_ids['Entrez Gene ID'], login=self.login)

        # add on human specific gene statements
        for key in ['HGNC ID', 'HGNC Gene Symbol']:
            if key in self.external_ids:
                s.append(wdi_core.WDString(self.external_ids[key], PROPS[key], references=[self.entrez_ref]))

        # add on gene position statements
        if 'genomic_pos' in self.record:
            ss = self.do_gp_human()
            if ss:
                s.extend(ss)

        return s
        '''

    def validate_record(self):
        assert 'locus_tag' in self.record
        assert 'HGNC' in self.record
        assert 'symbol' in self.record
        assert 'ensembl' in self.record and 'transcript' in self.record['ensembl']
        assert 'refseq' in self.record and 'rna' in self.record['ensembl']
        assert 'alias' in self.record

    '''
    def create_gp_statements_chr(self):
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
    '''