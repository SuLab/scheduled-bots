# http://185.54.114.71:8181

# need to import all item stubs for: genes

from wikibase_tools import EntityMaker
from itertools import chain
import pandas as pd

from scheduled_bots.gene_disease_phenotype.generate_omim_tsv import parse_genemap2_table
from wikidataintegrator import wdi_core, wdi_helpers

mediawiki_api_url = "http://185.54.114.71:8181/w/api.php"
sparql_endpoint_url = "http://185.54.114.71:8282/proxy/wdqs/bigdata/namespace/wdq/sparql"
username = "testbot"
password = "password"
maker = EntityMaker(mediawiki_api_url, sparql_endpoint_url, username, password)
h = wdi_helpers.WikibaseHelper(sparql_endpoint_url)


# login = wdi_login.WDLogin("testbot", "password", mediawiki_api_url=mediawiki_api_url)
# wdi_core.WDItemEngine.DISTINCT_VALUE_PROPS[sparql_endpoint_url] = {''}
# maker.make_entities(['P2302', 'Q21502410'])

#### genes
df = parse_genemap2_table('genemap2.txt')
all_hgnc = set(df.gene_symbol)
existing_hgnc = h.id_mapper("P353")
all_hgnc = all_hgnc - set(existing_hgnc.keys())
all_hgnc_wd_qid = wdi_helpers.get_values("P353", all_hgnc)
maker.make_entities(sorted(all_hgnc_wd_qid.values()))

### pmids
dfdp = pd.read_csv("phenotype.hpoa", sep='\t')
dfdp['disease_curie'] = dfdp['#DB'].map(str) + ":" + dfdp['DB_Object_ID'].map(str)
dfdp = dfdp.query("Aspect == 'P'")
dfdp = dfdp[dfdp.Frequency.isnull() | dfdp.Frequency.str.startswith("HP:")]
dfdp = dfdp[dfdp.Qualifier.isnull()]
dfdp = dfdp[dfdp['#DB'].isin({'OMIM', 'ORPHA'})]
all_refs = set(chain(*dfdp.DB_Reference.str.split(";")))
all_pmids = set(x[5:] for x in all_refs if x.startswith("PMID:"))

existing_pmid = h.id_mapper("P698")
existing_pmid = existing_pmid if existing_pmid else dict()
all_pmids = all_pmids - set(existing_pmid.keys())
all_pmids_wd_qid = wdi_helpers.get_values("P698", all_pmids)
maker.make_entities(sorted(all_pmids_wd_qid.values()))

# other items
qids = ['Q23190853', 'Q23190881', 'Q55239025', 'Q23190856', 'Q17027854']
maker.make_entities(qids)