import argparse
import os

from scheduled_bots.ontology.obographs import Graph, Node
from wikidataintegrator import wdi_login
from scheduled_bots import PROPS


class DONode(Node):
    def set_aliases(self, wd_item):
        # filter out aliases containing these strings
        bad_things = {'(disorder)', '[obs]', '(finding)', 'subac.', 'morphologic abnormality', '(qualifier value)', '[dup]'}
        if self.synonyms is not None:
            aliases = [x for x in self.synonyms if all(y not in x for y in bad_things)]
            wd_item.set_aliases(aliases=aliases, append=True)

    def _pre_create(self):
        # remove SNOMEDCT_US_2016_03_01 xrefs
        prefixes = {'SNOMED', 'EFO:', 'KEGG:', 'MEDDRA:', 'CSP:', 'URL:'}
        self.xrefs = set(x for x in self.xrefs if all(not x.lower().startswith(prefix.lower()) for prefix in prefixes))


class DOGraph(Graph):
    NAME = "Disease Ontology"
    GRAPH_URI = 'http://purl.obolibrary.org/obo/doid.owl'
    QID = "Q5282129"
    DEFAULT_DESCRIPTION = "human disease"
    APPEND_PROPS = {PROPS['subclass of'], PROPS['instance of'],
                    PROPS['has cause'], PROPS['anatomical location'],
                    PROPS['has phenotype'], PROPS['pathogen transmission process'],
                    PROPS['OMIM ID'], PROPS['Orphanet ID'],
                    PROPS['MeSH ID'], PROPS['ICD-10-CM'],
                    PROPS['ICD-10'], PROPS['ICD-9-CM'],
                    PROPS['ICD-9'], PROPS['NCI Thesaurus ID'],
                    PROPS['UMLS CUI'], PROPS['Disease Ontology ID'],
                    PROPS['GARD rare disease ID'], PROPS['Human Phenotype Ontology ID']}
    FAST_RUN = True

    PRED_PID_MAP = {'http://purl.obolibrary.org/obo/RO_0001025': PROPS['anatomical location'],#anatomical location doesn't have a RO#. This RO# is 'located_in'
                    'http://purl.obolibrary.org/obo/RO_0002200': PROPS['has phenotype'],  # has phenotype
                    # 'http://purl.obolibrary.org/obo/IDO_0000664': PROPS[],  # has_material_basis_in
                    # 'http://purl.obolibrary.org/obo/RO_0003304': PROPS[],  # contributes to condition
                    'http://purl.obolibrary.org/obo/RO_0002451': PROPS['pathogen transmission process'],  # transmitted by
                    # 'http://purl.obolibrary.org/obo/RO_0001020': PROPS[],  # is allergic trigger for
                    'is_a': PROPS['subclass of']}

    NODE_CLASS = DONode

    def filter_nodes(self):
        super(DOGraph, self).filter_nodes()
        self.nodes = [x for x in self.nodes if "DOID:" in x.id_curie]

    def _post_parse_graph(self):
        # we need to get edges out of the logicalDefinitionAxioms, and im not sure how this will generalize ...
        # also, this is not going to work on, for example: http://purl.obolibrary.org/obo/DOID_5520
        # which has multiple values. will have to do with sparql at some point # todo

        # location
        edges_location = []
        for axiom in self.json_graph['logicalDefinitionAxioms']:
            for restriction in axiom['restrictions']:
                if restriction and (restriction['propertyId'] == "http://purl.obolibrary.org/obo/RO_0001025"):
                    edges_location.append({'sub': axiom['definedClassId'],
                                           'pred': restriction['propertyId'],
                                           'obj': restriction['fillerId']})
        self.edges.extend(edges_location)
        # logical definition axioms are not all encompassing. A lot of information is stored in 'edges'.

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='run wikidata disease ontology bot')
    parser.add_argument("json_path", help="Path to json file")
    parser.add_argument("--local", help="preconfigured local wikibase port 7171 and 7272", action='store_true')
    args = parser.parse_args()

    if args.local:
        mediawiki_api_url = "http://localhost:7171/w/api.php"
        sparql_endpoint_url = "http://localhost:7272/proxy/wdqs/bigdata/namespace/wdq/sparql"
        login = wdi_login.WDLogin("testbot", "password", mediawiki_api_url=mediawiki_api_url)
    else:
        try:
            from scheduled_bots.local import WDUSER, WDPASS
        except ImportError:
            if "WDUSER" in os.environ and "WDPASS" in os.environ:
                WDUSER = os.environ['WDUSER']
                WDPASS = os.environ['WDPASS']
            else:
                raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

        mediawiki_api_url = 'https://www.wikidata.org/w/api.php'
        sparql_endpoint_url = 'https://query.wikidata.org/sparql'
        login = wdi_login.WDLogin(WDUSER, WDPASS)



    g = DOGraph(args.json_path, mediawiki_api_url=mediawiki_api_url, sparql_endpoint_url=sparql_endpoint_url)
    g.run(login)
    
print("Done running obographs_DO")