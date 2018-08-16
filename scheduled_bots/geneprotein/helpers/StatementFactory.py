from wikidataintegrator import wdi_core
from scheduled_bots import PROPS
from scheduled_bots.geneprotein import type_of_gene_map

source_ref_id = {
        'ensembl': "Ensembl Gene ID",
        'entrez': 'Entrez Gene ID'
}

class StatementFactory:

    def create_statements(self, record, organism_info,
                          external_ids, entrez_ref, ensembl_ref, refseq_ref, refseq=False, deprecated_entrez=False):
        """
        create statements common to all genes
        """
        s = []

        ############
        # ID statements (required)
        ############
        if entrez_ref:
            rank = 'normal'
            if deprecated_entrez:
                rank = 'deprecated'
            s.append(wdi_core.WDString(external_ids['Entrez Gene ID'], PROPS['Entrez Gene ID'], rank=rank,
                                       references=[entrez_ref]))

        # optional ID statements
        if ensembl_ref:
            for ensembl_gene_id in external_ids['Ensembl Gene ID']:
                s.append(wdi_core.WDString(ensembl_gene_id, PROPS['Ensembl Gene ID'], references=[ensembl_ref]))

            if 'Ensembl Transcript ID' in external_ids:
                for id in external_ids['Ensembl Transcript ID']:
                    s.append(wdi_core.WDString(id, PROPS['Ensembl Transcript ID'], references=[ensembl_ref]))

        key = 'RefSeq RNA ID'
        if key in external_ids and entrez_ref:
            for id in external_ids[key]:
                s.append(wdi_core.WDString(id, PROPS[key], references=[entrez_ref]))

        for key in ['Saccharomyces Genome Database ID', 'Mouse Genome Informatics ID',
                    'MGI Gene Symbol', 'HomoloGene ID', 'Rat Genome Database ID', 'FlyBase Gene ID',
                    'Wormbase Gene ID', 'ZFIN Gene ID', 'cytogenetic location']:
            if key in external_ids:
                refs = []
                if entrez_ref is not None:
                    refs = [entrez_ref]
                s.append(wdi_core.WDString(external_ids[key], PROPS[key], references=refs))

        if 'NCBI Locus tag' in external_ids:
            if entrez_ref:
                ref = entrez_ref
                if refseq and refseq_ref:
                    ref = refseq_ref
            elif refseq_ref:
                ref = refseq_ref
            s.append(wdi_core.WDString(external_ids['NCBI Locus tag'], PROPS['NCBI Locus tag'], references=[ref]))

        if 'Refseq Genome ID' in external_ids:
            if refseq_ref:
                ref = refseq_ref
            s.append(
                wdi_core.WDString(external_ids['Refseq Genome ID'], PROPS['Refseq Genome ID'], references=[ref]))

        ############
        # Gene statements
        ############
        # if there is an ensembl ID, this comes from ensembl, otherwise, entrez
        gene_refs = []
        if ensembl_ref:
            gene_refs = [ensembl_ref]
        elif entrez_ref:
            gene_refs = [entrez_ref]
        elif refseq_ref:
            gene_refs = [refseq_ref]

        if refseq and refseq_ref:
            gene_refs = [refseq_ref]

        # instance of gene, ncRNA.. etc
        type_of_gene = record['type_of_gene']['@value']
        assert type_of_gene in type_of_gene_map, "unknown type of gene: {}".format(type_of_gene)
        type_of_gene = type_of_gene
        # "protein-coding gene" will be instance of "gene"
        s.append(wdi_core.WDItemID(type_of_gene_map[type_of_gene], PROPS['instance of'], references=gene_refs))

        if type_of_gene not in {'protein-coding', 'pseudo', 'other', 'unknown'}:
            # make sure we add instance of "gene" as well
            s.append(wdi_core.WDItemID("Q7187", PROPS['instance of'], references=gene_refs))

        # found in taxon
        s.append(wdi_core.WDItemID(organism_info['wdid'], PROPS['found in taxon'], references=gene_refs))

        return s

class ChromosomalStatementFactory(StatementFactory):

    def __init__(self, chr_num_wdid):
        self.chr_num_wdid = chr_num_wdid

    def create_statements(self, record, organism_info,
                          external_ids, entrez_ref, ensembl_ref, refseq_ref, refseq=False, deprecated_entrez=False):
        """
        Create genomic_pos start stop orientation on a chromosome
        :return:
        """

        s = super().create_statements(record, organism_info, external_ids, entrez_ref, ensembl_ref, refseq_ref, refseq, deprecated_entrez)

        genomic_pos_values = record['genomic_pos']['@value']
        #genomic_pos_source = record['genomic_pos']['@source']

        if entrez_ref:
            genomic_pos_refs = [entrez_ref]
        elif ensembl_ref:
            genomic_pos_refs = [ensembl_ref]
        elif refseq_ref:
            genomic_pos_refs = [refseq_ref]
        else:
            print("No genomic position ref")
            genomic_pos_refs = []

        if refseq and refseq_ref:
            genomic_pos_refs = [refseq_ref]

        all_chr = set([self.chr_num_wdid[x['chr']] for x in genomic_pos_values])
        all_strand = set(['Q22809680' if x['strand'] == 1 else 'Q22809711' for x in genomic_pos_values])

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

class HumanStatementFactory(ChromosomalStatementFactory):

    def create_statements(self, record, organism_info,
                          external_ids, entrez_ref, ensembl_ref, refseq_ref, refseq=False, deprecated_entrez=False):
        """
       create genomic pos, chr, strand statements for human
       includes genomic assembly

       genes that are on an unlocalized scaffold will have no genomic position statements
       example: https://mygene.info/v3/gene/102724770
       https://www.wikidata.org/wiki/Q20970159
       :return:
       """
        s = super().create_statements(record, organism_info, external_ids, entrez_ref, ensembl_ref, refseq_ref, refseq,
                                  deprecated_entrez)
        # add on human specific gene statements
        for key in ['HGNC ID', 'HGNC Gene Symbol']:
            if key in external_ids:
                s.append(wdi_core.WDString(external_ids[key], PROPS[key], references=[entrez_ref]))

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