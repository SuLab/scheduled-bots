import argparse
import os

from scheduled_bots.ontology.obographs import Graph, Node
from wikidataintegrator import wdi_login, wdi_core
from scheduled_bots import PROPS


def ec_formatter(ec_number):
    splits = ec_number.split('.')
    if len(splits) < 4:
        for x in range(4 - len(splits)):
            splits.append('-')

    return '.'.join(splits)


class GONode(Node):
    def _pre_create(self):
        super(GONode, self)._pre_create()
        self.xrefs = list(self.xrefs)
        for n, xref in enumerate(self.xrefs):
            if xref.startswith("EC:"):
                print(xref)
                prefix, ec_number = xref.split(":")
                self.xrefs[n] = ":".join([prefix, ec_formatter(ec_number)])
                print(xref)
        self.xrefs = set(self.xrefs)


class GOGraph(Graph):
    NAME = "Gene Ontology"
    GRAPH_URI = 'http://purl.obolibrary.org/obo/go.owl'
    QID = "Q135085"
    DEFAULT_DESCRIPTION = ""
    APPEND_PROPS = {PROPS['subclass of'], PROPS['instance of'],
                    PROPS['has cause'], PROPS['location'], PROPS['part of'],
                    PROPS['has part'], PROPS['regulates (molecular biology)'],
                    PROPS['Gene Ontology ID']}
    FAST_RUN = True

    PRED_PID_MAP = {
        'is_a': PROPS['subclass of'],
        'http://purl.obolibrary.org/obo/BFO_0000050': PROPS['part of'],
        'http://purl.obolibrary.org/obo/BFO_0000051': PROPS['has part'],
        'http://purl.obolibrary.org/obo/RO_0002211': PROPS['regulates (molecular biology)'],  # regulates
        'http://purl.obolibrary.org/obo/RO_0002212': None,  # negatively regulates
        'http://purl.obolibrary.org/obo/RO_0002213': None,  # positively regulates
    }

    NODE_CLASS = GONode

    regulates = {
        'http://purl.obolibrary.org/obo/RO_0002212': 'http://purl.obolibrary.org/obo/GO_0048519',
        'http://purl.obolibrary.org/obo/RO_0002213': 'http://purl.obolibrary.org/obo/GO_0048518'
    }

    def make_statement_from_edge(self, edge):
        # custom statement creator for regulates
        h = self.helper
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

    g = GOGraph(args.json_path, mediawiki_api_url=mediawiki_api_url, sparql_endpoint_url=sparql_endpoint_url)
    g.run(login)
