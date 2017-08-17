import pandas as pd
df = pd.read_csv("cgi_biomarkers_per_variant.tsv", sep='\t')

# how do we handle combination variants?
# e.g. MET:amp;BRAF:V600E line 1000

alt_type_map = {
    'MUT': 'http://purl.obolibrary.org/obo/SO_0001878',
    'CNA': '',  # copy number variant?, could be:
    # deletion (http://purl.obolibrary.org/obo/SO_0001879) or amplification (http://purl.obolibrary.org/obo/SO_0001880)
    'FUS': 'http://purl.obolibrary.org/obo/SO_0001882',  # fusion
    'EXPR': 'http://purl.obolibrary.org/obo/SO_0001540',  # overexpression or underexpression
    'BIA': ''  # biallelic inactivation
}
