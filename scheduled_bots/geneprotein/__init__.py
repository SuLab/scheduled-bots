#########
# Helper functions
#########
type_of_gene_map = {'ncRNA': 'Q427087',
                    'snRNA': 'Q284578',
                    'snoRNA': 'Q284416',
                    'rRNA': 'Q215980',
                    'tRNA': 'Q201448',
                    'pseudo': 'Q277338',
                    'protein-coding': 'Q7187',  # replaced 'Q20747295'(protein coding gene) with gene
                    'other': 'Q7187',
                    'unknown': 'Q7187',
                    'gene': 'Q7187',
                    'miscRNA': 'Q11053',
                    'scRNA': 'Q25323710',
                    }

descriptions_by_type = {
    'ncRNA': 'non-coding RNA in the species {}',
    'snRNA': 'small nuclear RNA in the species {}',
    'snoRNA': 'small nucleolar RNA in the species {}',
    'rRNA': 'ribosomal RNA in the species {}',
    'tRNA': 'transfer RNA in the species {}',
    'pseudo': 'pseudogene in the species {}',
    'protein-coding': 'protein-coding gene in the species {}',
    'other': 'gene in the species {}',
    'unknown': 'genetic element in the species {}',
    'miscRNA': 'RNA in the species {}',
    'scRNA': 'small conditional RNA in the species {}'
}

human_chromosome_map = {
    '1': 'Q430258',
    '10': 'Q840737',
    '11': 'Q847096',
    '12': 'Q847102',
    '13': 'Q840734',
    '14': 'Q138955',
    '15': 'Q765245',
    '16': 'Q742870',
    '17': 'Q220677',
    '18': 'Q780468',
    '19': 'Q510786',
    '2': 'Q638893',
    '20': 'Q666752',
    '21': 'Q753218',
    '22': 'Q753805',
    '3': 'Q668633',
    '4': 'Q836605',
    '5': 'Q840741',
    '6': 'Q540857',
    '7': 'Q657319',
    '8': 'Q572848',
    '9': 'Q840604',
    'MT': 'Q27973632',
    'X': 'Q29867336',
    'Y': 'Q29867344'}

#########
# Mappings for GO
#########

go_props = {'MF': 'P680',
            'Function': 'P680',
            'F': 'P680',
            'CC': 'P681',
            'C': 'P681',
            'Component': 'P681',
            'BP': 'P682',
            'P': 'P682',
            'Process': 'P682'}

go_evidence_codes = {
    'EXP': 'Q23173789',
    'IDA': 'Q23174122',
    'IPI': 'Q23174389',
    'IMP': 'Q23174671',
    'IGI': 'Q23174952',
    'IEP': 'Q23175251',
    'ISS': 'Q23175558',
    'ISO': 'Q23190637',
    'ISA': 'Q23190738',
    'ISM': 'Q23190825',
    'IGC': 'Q23190826',
    'IBA': 'Q23190827',
    'IBD': 'Q23190833',
    'IKR': 'Q23190842',
    'IRD': 'Q23190850',
    'RCA': 'Q23190852',
    'TAS': 'Q23190853',
    'NAS': 'Q23190854',
    'IC': 'Q23190856',
    'ND': 'Q23190857',
    'IEA': 'Q23190881',
    'IMR': 'Q23190842'
}

###############
# For references
###############
# wd item representing a source database
sources_wdids = {'UniProt': 'Q905695',
                 'Uniprot': 'Q905695',
                 'UniProtKB': 'Q905695',
                 'ncbi_gene': 'Q20641742',  # these two are the same?  --v
                 'Entrez': 'Q20641742',
                 'ncbi_taxonomy': 'Q13711410',
                 'swiss_prot': 'Q2629752',
                 'trembl': 'Q22935315',
                 'Ensembl': 'Q1344256',
                 'refseq': 'Q7307074'
                 }

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
         'encoded by': 'P702',
         'RefSeq Protein ID': 'P637',
         'UniProt ID': 'P352',
         'Ensembl Protein ID': 'P705',
         'OMIM ID': 'P492',
         'NCBI Taxonomy ID': 'P685'
         }

# http://www.geneontology.org/doc/GO.xrf_abbs
curators_wdids = {'AgBase': 'Q4690901',
                  'Alzheimers_University_of_Toronto': 'Q28122976',
                  'BHF-UCL': 'Q4970039',
                  'CACAO': 'Q27929332',
                  'CAFA': 'Q29976522',
                  'CollecTF': 'Q17083998',
                  'DFLAT': 'Q28122980',
                  'dictyBase': 'Q5273990',
                  'EnsemblPlants': 'Q27927711',
                  'Ensembl': 'Q1344256',
                  'FlyBase': 'Q3074571',
                  'GDB': 'Q5513070',
                  'GOC': 'Q23809253',
                  'GO_Central': 'Q27927716',
                  'HGNC': 'Q1646383',
                  'HPA': 'Q5937310',
                  'IntAct': 'Q27901835',
                  'InterPro': 'Q3047275',
                  'LIFEdb': 'Q28122992',
                  'MGI': 'Q1951035',
                  'NTNU_SB': 'Q28122995',
                  'ParkinsonsUK-UCL': 'Q27929334',
                  'PINC': 'Q28122996',
                  'Reactome': 'Q2134522',
                  'SGD': 'Q3460832',
                  'SYSCILIA_CCNET': 'Q28122997',
                  'UniProt': 'Q905695',
                  'WormBase': 'Q3570042'}

# These are for reference external IDs to use for GO annotations curators
curator_ref = {'SGD': 'Saccharomyces Genome Database ID',
               'MGI': 'Mouse Genome Informatics ID',
               'UniProt': 'UniProt ID', }

#########
# Organism Info
#########
organisms_info = {
    559292: {
        "type": "fungal",
        "name": "Saccharomyces cerevisiae S288c",
        "wdid": "Q27510868",
        'taxid': 559292
    },
    9606: {
        "name": "Homo sapiens",
        "type": "mammalian",
        "wdid": "Q15978631",
        'taxid': 9606
    },
    10090: {
        "name": "Mus musculus",
        "type": "mammalian",
        "wdid": "Q83310",
        'taxid': 10090
    },
    10116: {
        "name": "Rattus norvegicus",
        "type": "mammalian",
        "wdid": "Q184224",
        'taxid': 10116
    },
    9545: {
        "name": "Macaca nemestrina",
        "type": "mammalian",
        "wdid": "Q618026",
        'taxid': 9545
    },
    3702: {
        "name": "Arabidopsis thaliana",
        "type": "plant",
        "wdid": "Q158695",
        'taxid': 3702
    },
    7227: {
        "name": "Drosophila melanogaster",
        "type": None,
        "wdid": "Q130888",
        'taxid': 7227
    },
    6239: {
        "name": "Caenorhabditis elegans",
        "type": None,
        "wdid": "Q91703",
        'taxid': 6239
    },
    7955: {
        "name": "Danio rerio",  # zebrafish
        "type": None,
        "wdid": "Q169444",
        'taxid': 7955
    },
}
