from scheduled_bots.ontology.obographs import Graph, Node
from wikidataintegrator import wdi_login

JSON_PATH = "go.json"
GRAPH_URI = 'http://purl.obolibrary.org/obo/go.owl'

from scheduled_bots.local import WDUSER, WDPASS
from scheduled_bots import PROPS


class GOGraph(Graph):
    NAME = "Gene Ontology"
    QID = "Q135085"
    DEFAULT_DESCRIPTION = ""
    APPEND_PROPS = {PROPS['subclass of'], PROPS['instance of'],
                    PROPS['has cause'], PROPS['location'],
                    PROPS['OMIM ID'], PROPS['Orphanet ID'],
                    PROPS['MeSH ID'], PROPS['ICD-10-CM'],
                    PROPS['ICD-10'], PROPS['ICD-9-CM'],
                    PROPS['ICD-9'], PROPS['NCI Thesaurus ID'],
                    PROPS['UMLS CUI']}
    FAST_RUN = True
    FAST_RUN_FILTER = {PROPS['Gene Ontology ID']: ''}

    PRED_PID_MAP = {
        'is_a': PROPS['subclass of']
    }

    NAMESPACE_QID = {
        'cellular_component': 'Q5058355',
        'biological_process': '',
        'molecular_function': ''
    }

login = wdi_login.WDLogin(WDUSER, WDPASS)
g = GOGraph()
g.parse_graph(JSON_PATH, GRAPH_URI)

# g.create_release(login)
# g.create_nodes(login, write=False)
