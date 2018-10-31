from scheduled_bots import PROPS
import argparse
import os
from scheduled_bots.ontology.obographs import Graph, Node
from wikidataintegrator import wdi_login


class HPNode(Node):
    def _pre_create(self):
        # keep only these
        prefixes = {'SNOMED:', 'UMLS:', 'MSH:', 'ICD-10:', 'NCIT:', 'ORPHA:'}
        self.xrefs = set(x for x in self.xrefs if any(x.lower().startswith(prefix.lower()) for prefix in prefixes))


class HPGraph(Graph):
    NAME = "Human Phenotype Ontology"
    QID = "Q17027854"
    GRAPH_URI = 'http://purl.obolibrary.org/obo/hp.owl'
    DEFAULT_DESCRIPTION = "human phenotype"
    APPEND_PROPS = {PROPS['subclass of'], PROPS['instance of'], PROPS['MeSH ID'],
                    PROPS['Human Phenotype Ontology ID'], PROPS['UMLS CUI']}
    CORE_IDS = {PROPS['Human Phenotype Ontology ID'], PROPS['MeSH ID']}
    FAST_RUN = True
    EXCLUDE_NODES = {'http://purl.obolibrary.org/obo/HP_0000001'}

    PRED_PID_MAP = {
        'is_a': PROPS['subclass of'],
    }
    NODE_CLASS = HPNode


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='run wikidata Human Phenotype ontology bot')
    parser.add_argument("json_path", help="Path to json file")
    parser.add_argument("--local", help="preconfigured local wikibase port 7171 and 7272", action='store_true')
    args = parser.parse_args()

    if args.local:
        mediawiki_api_url = "http://185.54.114.71:8181/w/api.php"
        sparql_endpoint_url = "http://185.54.114.71:8282/proxy/wdqs/bigdata/namespace/wdq/sparql"
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

    g = HPGraph(args.json_path, mediawiki_api_url=mediawiki_api_url, sparql_endpoint_url=sparql_endpoint_url)
    g.run(login)
