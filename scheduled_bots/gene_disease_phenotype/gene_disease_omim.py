"""

Gene to disease (omim)
https://data.omim.org/downloads/{secret_key}/genemap2.txt


"""

from scheduled_bots.gene_disease_phenotype.generate_omim_tsv import parse_genemap2_table



# gene -> disease (omim)
dfgd_omim = parse_genemap2_table('genemap2.txt')