# Helper functions
#########

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

# property ID
prop_ids = {'uniprot': 'P352',
            'ncbi_gene': 'P351',
            'entrez_gene': 'P351',
            'ncbi_taxonomy': 'P685',
            'ncbi_locus_tag': 'P2393',
            'ensembl_gene': 'P594',
            'ensembl_protein': 'P705',
            'refseq_protein': 'P637'
            }

# http://www.geneontology.org/doc/GO.xrf_abbs
curators_wdids = {'AgBase': 'Q4690901',
                  'BHF-UCL': 'Q4970039',
                  'CACAO': 'Q27929332',
                  'EnsemblPlants': 'Q27927711',
                  'FlyBase': 'Q3074571',
                  'GOC': 'Q23809253',
                  'GO_Central': 'Q27927716',
                  'HGNC': 'Q1646383',
                  'IntAct': 'Q27901835',
                  'InterPro': 'Q3047275',
                  'MGI': 'Q1951035',
                  'ParkinsonsUK-UCL': 'Q27929334',
                  'Reactome': 'Q2134522',
                  'SGD': 'Q3460832',
                  'UniProt': 'Q905695',
                  'WormBase': 'Q3570042'}
