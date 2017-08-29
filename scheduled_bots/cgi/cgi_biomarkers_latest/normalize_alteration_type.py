import pandas as pd
df = pd.read_csv("cgi_biomarkers_per_variant.tsv", sep='\t')

# how do we handle combination variants?
# e.g. MET:amp;BRAF:V600E line 1000

alt_type_map = {
    'MUT': 'Q27429979',
    'CNA': '',  # copy number variant?, could be:
    # deletion (http://purl.obolibrary.org/obo/SO_0001879) or amplification (http://purl.obolibrary.org/obo/SO_0001880)
    'FUS': '',  # fusion
    'EXPR': '',  # overexpression or underexpression
    'BIA': ''  # biallelic inactivation
}
df['alteration_type_qid'] = df['Alteration type'].map(alt_type_map.get)
df.to_csv("cgi_biomarkers_per_variant.tsv", sep="\t", index=None)
