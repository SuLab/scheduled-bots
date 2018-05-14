from tqdm import tqdm

from scheduled_bots.ontology.obographs import Graph, Node
from wikidataintegrator import wdi_login

JSON_PATH = "hp.json"
GRAPH_URI = 'http://purl.obolibrary.org/obo/hp.owl'

mediawiki_api_url = "http://localhost:7171/w/api.php"
sparql_endpoint_url = "http://localhost:7272/proxy/wdqs/bigdata/namespace/wdq/sparql"
login = wdi_login.WDLogin("testbot", "password", mediawiki_api_url=mediawiki_api_url)

if False:
    mediawiki_api_url = 'https://www.wikidata.org/w/api.php'
    sparql_endpoint_url = 'https://query.wikidata.org/sparql'
    from scheduled_bots.local import WDUSER, WDPASS
    login = wdi_login.WDLogin(WDUSER, WDPASS)

from scheduled_bots import PROPS
from wikidataintegrator.wdi_helpers import WikibaseHelper

h = WikibaseHelper(sparql_endpoint_url)


class HPGraph(Graph):
    NAME = "Human Phenotype Ontology"
    QID = h.get_qid("Q17027854")
    DEFAULT_DESCRIPTION = "human phenotype"
    APPEND_PROPS = {h.get_pid(x) for x in {PROPS['subclass of'], PROPS['instance of'],
                                           PROPS['MeSH ID'], PROPS['Human Phenotype Ontology ID']}}
    CORE_PROPS = {h.get_pid(x) for x in {PROPS['Human Phenotype Ontology ID']}}
    FAST_RUN = True
    FAST_RUN_FILTER = {h.get_pid(PROPS['Human Phenotype Ontology ID']): ''}
    EXCLUDE_NODES = {'http://purl.obolibrary.org/obo/HP_0000001'}

    # TODO: the instance of statement should be 'Phenotype' (Q104053), which is in UPHENO, not HP
    # so I'm not sure how to handle this as we don't have that prop in wikidata

    PRED_PID_MAP = {
        'is_a': h.get_pid(PROPS['subclass of']),
        'http://purl.obolibrary.org/obo/BFO_0000050': h.get_pid(PROPS['part of']),
        'http://purl.obolibrary.org/obo/RO_0002202': h.get_pid(PROPS['develops from']),
    }
if __name__ == "__main__":
    g = HPGraph(JSON_PATH, GRAPH_URI, mediawiki_api_url=mediawiki_api_url, sparql_endpoint_url=sparql_endpoint_url)

    g.create_release(login)
    # g.nodes = g.nodes[:500]
    g.create_nodes(login)

    g.create_edges(login)

    g.check_for_existing_deprecated_nodes()
    g.remove_deprecated_statements(login)
