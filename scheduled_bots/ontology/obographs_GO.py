from scheduled_bots.ontology.obographs import Graph, Node
from wikidataintegrator import wdi_login, wdi_core

JSON_PATH = "go.json"
GRAPH_URI = 'http://purl.obolibrary.org/obo/go.owl'

from scheduled_bots import PROPS

mediawiki_api_url = "http://localhost:7171/w/api.php"
sparql_endpoint_url = "http://localhost:7272/proxy/wdqs/bigdata/namespace/wdq/sparql"
login = wdi_login.WDLogin("testbot", "password", mediawiki_api_url=mediawiki_api_url)

if False:
    mediawiki_api_url = 'https://www.wikidata.org/w/api.php'
    sparql_endpoint_url = 'https://query.wikidata.org/sparql'
    from scheduled_bots.local import WDUSER, WDPASS

    login = wdi_login.WDLogin(WDUSER, WDPASS)

from wikidataintegrator.wdi_helpers import WikibaseHelper

h = WikibaseHelper(sparql_endpoint_url)


class GOGraph(Graph):
    NAME = "Gene Ontology"
    QID = h.get_qid("Q135085")
    DEFAULT_DESCRIPTION = ""
    APPEND_PROPS = {h.get_pid(x) for x in {PROPS['subclass of'], PROPS['instance of'],
                                           PROPS['has cause'], PROPS['location'], PROPS['part of'],
                                           PROPS['has part'], PROPS['regulates (molecular biology)']}}
    FAST_RUN = True
    FAST_RUN_FILTER = {h.get_pid(PROPS['Gene Ontology ID']): ''}

    PRED_PID_MAP = {
        'is_a': h.get_pid(PROPS['subclass of']),
        'http://purl.obolibrary.org/obo/BFO_0000050': h.get_pid(PROPS['part of']),
        'http://purl.obolibrary.org/obo/BFO_0000051': h.get_pid(PROPS['has part']),
        'http://purl.obolibrary.org/obo/RO_0002211': h.get_pid(PROPS['regulates (molecular biology)']),  # regulates
        'http://purl.obolibrary.org/obo/RO_0002212': None,  # negatively regulates
        'http://purl.obolibrary.org/obo/RO_0002213': None,  # positively regulates
    }

    regulates = {
        'http://purl.obolibrary.org/obo/RO_0002212': 'http://purl.obolibrary.org/obo/GO_0048519',
        'http://purl.obolibrary.org/obo/RO_0002213': 'http://purl.obolibrary.org/obo/GO_0048518'
    }

    def make_statement_from_edge(self, edge):
        # custom statement creator for regulates
        if edge['pred'] in {'http://purl.obolibrary.org/obo/RO_0002212', 'http://purl.obolibrary.org/obo/RO_0002213'}:
            subj_node = self.uri_node_map[edge['sub']]
            obj_qid = self.get_object_qid(edge['obj'])
            # print(obj_qid, edge['pred'])
            qual_qid = self.uri_node_map[self.regulates[edge['pred']]].qid
            qualifier = wdi_core.WDItemID(qual_qid, h.get_pid(PROPS['subject has role']),
                                          is_qualifier=True)
            pred_pid = self.PRED_PID_MAP['http://purl.obolibrary.org/obo/RO_0002211']
            return wdi_core.WDItemID(obj_qid, pred_pid,
                                     references=[subj_node.create_ref_statement()],
                                     qualifiers=[qualifier])
        else:
            return super(GOGraph, self).make_statement_from_edge(edge)


g = GOGraph(mediawiki_api_url=mediawiki_api_url, sparql_endpoint_url=sparql_endpoint_url)
g.parse_graph(JSON_PATH, GRAPH_URI)

g.create_release(login)

g.create_nodes(login)

g.create_edges(login)
