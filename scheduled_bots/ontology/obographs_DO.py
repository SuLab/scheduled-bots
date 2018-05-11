from tqdm import tqdm

from scheduled_bots.ontology.obographs import Graph, Node
from wikidataintegrator import wdi_login

JSON_PATH = "doid.json"
GRAPH_URI = 'http://purl.obolibrary.org/obo/doid.owl'

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


class DONode(Node):
    def set_aliases(self, wd_item):
        # filter out aliases containing these strings
        bad_things = {'(disorder)', '[obs]'}
        if self.synonyms is not None:
            aliases = [x for x in self.synonyms if all(y not in x for y in bad_things)]
            wd_item.set_aliases(aliases=aliases, append=True)


class DOGraph(Graph):
    NAME = "Disease Ontology"
    QID = h.get_qid("Q5282129")
    DEFAULT_DESCRIPTION = "human disease"
    APPEND_PROPS = {h.get_pid(x) for x in {PROPS['subclass of'], PROPS['instance of'],
                                           PROPS['has cause'], PROPS['location'],
                                           PROPS['OMIM ID'], PROPS['Orphanet ID'],
                                           PROPS['MeSH ID'], PROPS['ICD-10-CM'],
                                           PROPS['ICD-10'], PROPS['ICD-9-CM'],
                                           PROPS['ICD-9'], PROPS['NCI Thesaurus ID'],
                                           PROPS['UMLS CUI'], PROPS['Disease Ontology ID']}}
    CORE_PROPS = {h.get_pid(x) for x in {PROPS['Disease Ontology ID']}}
    FAST_RUN = True
    FAST_RUN_FILTER = {h.get_pid(PROPS['Disease Ontology ID']): ''}

    PRED_PID_MAP = {'http://purl.obolibrary.org/obo/RO_0001025': h.get_pid(PROPS['location']),
                    # 'http://purl.obolibrary.org/obo/RO_0002200': PROPS[],  # has phenotype
                    # 'http://purl.obolibrary.org/obo/IDO_0000664': PROPS[],  # has_material_basis_in
                    # 'http://purl.obolibrary.org/obo/RO_0003304': PROPS[],  # contributes to condition
                    # 'http://purl.obolibrary.org/obo/RO_0002451': PROPS[],  # transmitted by
                    # 'http://purl.obolibrary.org/obo/RO_0001020': PROPS[],  # is allergic trigger for
                    'is_a': h.get_pid(PROPS['subclass of'])}

    NODE_CLASS = DONode

    def filter_nodes(self):
        super(DOGraph, self).filter_nodes()
        self.nodes = [x for x in self.nodes if "DOID:" in x.id_curie]


g = DOGraph(JSON_PATH, GRAPH_URI, mediawiki_api_url=mediawiki_api_url, sparql_endpoint_url=sparql_endpoint_url)

g.create_release(login)

g.create_nodes(login)

g.create_edges(login)

g.check_for_existing_deprecated_nodes()
g.remove_deprecated_statements(login)
