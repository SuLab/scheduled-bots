#########
# Helper functions
#########

type_of_gene_map = {'ncRNA': 'Q427087',
                    'snRNA': 'Q284578',
                    'snoRNA': 'Q284416',
                    'rRNA': 'Q215980',
                    'tRNA': 'Q201448',
                    'pseudo': 'Q277338',
                    'protein-coding': 'Q20747295'}


#########
# Mappings for GO
#########

go_props = {'MF': 'P680',
            'Function': 'P680',
            'CC': 'P681',
            'Component': 'P681',
            'BP': 'P682',
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
         }

# http://www.geneontology.org/doc/GO.xrf_abbs
curators_wdids = {'AgBase': 'Q4690901',
                  'Alzheimers_University_of_Toronto': 'Q28122976',
                  'BHF-UCL': 'Q4970039',
                  'CACAO': 'Q27929332',
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
               'UniProt': 'UniProt ID',}


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
    }
}
