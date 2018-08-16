from .Gene import Gene
from scheduled_bots import PROPS

class ChromosomalGene(Gene):
    """
    yeast, mouse, rat, worm, fly, zebrafish
    """

    def create_label(self):
        self.label = self.record['symbol']['@value']

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
