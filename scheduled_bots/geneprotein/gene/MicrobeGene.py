from .ChromosomalGene import ChromosomalGene

class MicrobeGene(ChromosomalGene):
    """
    Microbes

    """

    def create_label(self):
        name = self.record['name']['@value']
        if self.record['locus_tag']['@value'] not in name:
            name = name + " " + self.record['locus_tag']['@value']
        self.label = name

    def create_description(self):
        if self.organism_info['type']:
            self.description = '{} gene found in {}'.format(self.organism_info['type'], self.organism_info['name'])
        else:
            self.description = 'Gene found in {}'.format(self.organism_info['name'])

    def validate_record(self):
        pass

    '''
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
'''