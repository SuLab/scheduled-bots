from tqdm import tqdm

from scheduled_bots.ontology.obographs import Graph, Node
from wikidataintegrator import wdi_login

JSON_PATH = "uberon.json"
GRAPH_URI = 'http://purl.obolibrary.org/obo/uberon.owl'

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


class UberonGraph(Graph):
    NAME = "Uberon"
    QID = h.get_qid("Q7876491")
    DEFAULT_DESCRIPTION = "body part"
    APPEND_PROPS = {h.get_pid(x) for x in {PROPS['subclass of'], PROPS['instance of'],
                                           PROPS['MeSH ID'], PROPS['UBERON ID']}}
    CORE_PROPS = {h.get_pid(x) for x in {PROPS['UBERON ID']}}
    FAST_RUN = True
    FAST_RUN_FILTER = {h.get_pid(PROPS['UBERON ID']): ''}

    PRED_PID_MAP = {
        'is_a': h.get_pid(PROPS['subclass of']),
        'http://purl.obolibrary.org/obo/BFO_0000050': h.get_pid(PROPS['part of']),
        'http://purl.obolibrary.org/obo/RO_0002202': h.get_pid(PROPS['develops from']),
    }
if __name__ == "__main__":
    g = UberonGraph(JSON_PATH, GRAPH_URI, mediawiki_api_url=mediawiki_api_url, sparql_endpoint_url=sparql_endpoint_url)

    g.create_release(login)
    # g.nodes = g.nodes[:500]
    g.create_nodes(login)

    g.create_edges(login)

    g.check_for_existing_deprecated_nodes()
    g.remove_deprecated_statements(login)
