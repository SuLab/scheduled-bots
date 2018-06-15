import argparse
import os
from datetime import datetime

from scheduled_bots.ontology.obographs import Graph, Node
from wikidataintegrator import wdi_login
from scheduled_bots import PROPS


class SONode(Node):
    def _pre_create(self):
        # remove all xrefs
        self.xrefs = set()

    def set_label(self, wd_item):
        # fix underscores first
        self.label = self.label.replace("_", " ")
        super(SONode, self).set_label(wd_item)

    def set_descr(self, wd_item):
        self.descr = self.label.replace("_", " ")
        super(SONode, self).set_descr(wd_item)

    def set_aliases(self, wd_item):
        # get rid of any with a colon in them
        if self.synonyms is not None:
            self.synonyms = set([x.replace("_", " ") for x in self.synonyms if ":" not in x])
        super(SONode, self).set_aliases(wd_item)


class SOGraph(Graph):
    NAME = "Sequence Ontology"
    GRAPH_URI = 'http://purl.obolibrary.org/obo/so.owl'
    QID = "Q7452458"
    DEFAULT_DESCRIPTION = "sequence ontology term"
    APPEND_PROPS = {PROPS['Sequence Ontology ID']}
    FAST_RUN = True

    PRED_PID_MAP = {'is_a': PROPS['subclass of']}

    NODE_CLASS = SONode

    def parse_meta(self):
        meta = self.json_graph['meta']
        # this is a PURI to the release owl file, but is different than expected...
        # e.g. : http://purl.obolibrary.org/obo/so/so-xp/releases/2015-11-24/so-xp.owl/so.owl
        self.version = meta['version']
        self.edition = self.version.rsplit("/", 3)[1]
        self.date = datetime.strptime(self.edition, '%Y-%m-%d')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='run wikidata sequence ontology bot')
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

    g = SOGraph(args.json_path, mediawiki_api_url=mediawiki_api_url, sparql_endpoint_url=sparql_endpoint_url)
    g.run(login)
