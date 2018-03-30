from tqdm import tqdm

from scheduled_bots.ontology.obographs import Graph, Node
from wikidataintegrator import wdi_login

# JSON_PATH = "hp.json"
# GRAPH_URI = 'http://purl.obolibrary.org/obo/hp.owl'

JSON_PATH = "doid.json"
GRAPH_URI = 'http://purl.obolibrary.org/obo/doid.owl'

from scheduled_bots.local import WDUSER, WDPASS
from scheduled_bots import PROPS


class DONode(Node):
    def set_aliases(self, wd_item):
        # filter out aliases containing these strings
        bad_things = {'(disorder)', '[obs]'}
        if self.synonyms is not None:
            aliases = [x for x in self.synonyms if all(y not in x for y in bad_things)]
            wd_item.set_aliases(aliases=aliases, append=True)


class DOGraph(Graph):
    NAME = "Disease Ontology"
    QID = "Q5282129"
    DEFAULT_DESCRIPTION = "human disease"
    APPEND_PROPS = {PROPS['subclass of'], PROPS['instance of'],
                    PROPS['has cause'], PROPS['location'],
                    PROPS['OMIM ID'], PROPS['Orphanet ID'],
                    PROPS['MeSH ID'], PROPS['ICD-10-CM'],
                    PROPS['ICD-10'], PROPS['ICD-9-CM'],
                    PROPS['ICD-9'], PROPS['NCI Thesaurus ID'],
                    PROPS['UMLS CUI']}
    FAST_RUN = True
    FAST_RUN_FILTER = {PROPS['Disease Ontology ID']: ''}

    PRED_PID_MAP = {'http://purl.obolibrary.org/obo/RO_0001025': PROPS['location'],
                    # 'http://purl.obolibrary.org/obo/RO_0002200': PROPS[],  # has phenotype
                    # 'http://purl.obolibrary.org/obo/IDO_0000664': PROPS[],  # has_material_basis_in
                    # 'http://purl.obolibrary.org/obo/RO_0003304': PROPS[],  # contributes to condition
                    # 'http://purl.obolibrary.org/obo/RO_0002451': PROPS[],  # transmitted by
                    # 'http://purl.obolibrary.org/obo/RO_0001020': PROPS[],  # is allergic trigger for
                    'is_a': PROPS['subclass of']}

    NAMESPACE_QID = {'disease_ontology': 'Q12136'}

    NODE_CLASS = DONode

    def filter_nodes(self):
        super(DOGraph, self).filter_nodes()
        self.nodes = [x for x in self.nodes if "DOID:" in x.id_curie]


login = wdi_login.WDLogin(WDUSER, WDPASS)
g = DOGraph()
g.parse_graph(JSON_PATH, GRAPH_URI)

g.create_release(login)
g.create_nodes(login, write=False)
g.create_edges(login, write=False)