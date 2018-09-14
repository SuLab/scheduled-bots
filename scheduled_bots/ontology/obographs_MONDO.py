"""
mondo-minimal.owl is missing
<owl:versionIRI rdf:resource="http://purl.obolibrary.org/obo/mondo/releases/2018-06-01/mondo.owl"/>

"""

import argparse
import os

from scheduled_bots import utils
from scheduled_bots.ontology.obographs import Graph, Node
from wikidataintegrator import wdi_login, wdi_helpers, wdi_core
from scheduled_bots import PROPS


class MondoNode(Node):
    mrh = None

    def set_aliases(self, wd_item):
        # filter out aliases containing these strings
        bad_things = {'(disorder)', '[obs]'}
        if self.synonyms is not None:
            aliases = [x for x in self.synonyms if all(y not in x for y in bad_things)]
            wd_item.set_aliases(aliases=aliases, append=True)

    def _pre_create(self):
        # remove leading zeros from GARD
        self.xrefs = set("GARD:" + str(int(xref.replace("GARD:", ""))) if xref.startswith("GARD:") else xref for xref in self.xrefs)

        # change ICD10 to ICD10CM
        self.xrefs = set([x.replace("ICD10:", "ICD10CM:").replace("ICD9:", "ICD9CM:") for x in self.xrefs])

    def create_xref_statements(self):
        """
        Special handling of xrefs to include mapping relation type
        """

        if not MondoNode.mrh:
            MondoNode.mrh = wdi_helpers.MappingRelationHelper(sparql_endpoint_url=self.graph.sparql_endpoint_url)

        # need a way to get from "http://linkedlifedata.com/resource/umls/id/C0265218" to UMLS:C0265218, for example
        # ignoring meddra and snomed
        uri_to_curie_map = {
            "http://identifiers.org/omim/": "OMIM:",
            "http://identifiers.org/mesh/": "MESH:",
            "http://linkedlifedata.com/resource/umls/id/": "UMLS:",
            "http://www.orpha.net/ORDO/Orphanet_": "Orphanet:",
            "http://purl.obolibrary.org/obo/DOID_": "DOID:",
            "http://purl.obolibrary.org/obo/NCIT_": "NCIT:"
        }

        def uri_to_curie(uri):
            for k, v in uri_to_curie_map.items():
                if k in uri:
                    uri = uri.replace(k, v)
                    return uri

        ss = []
        for mrt, xrefs in self.bpv.items():
            if mrt in self.mrh.ABV_MRT.values():
                for xref in xrefs:
                    xref_curie = uri_to_curie(xref)
                    if not xref_curie:
                        continue
                    self.xrefs.discard(xref_curie)
                    s = self.create_xref_statement(xref_curie)
                    if not s:
                        continue
                    self.mrh.set_mrt(s, mrt)
                    ss.append(s)
        for xref in self.xrefs:
            s = self.create_xref_statement(xref)
            if s:
                ss.append(s)

        return ss

    def set_descr(self, wd_item):
        # if the new description is "human disease" and there exists a description that isn't blank, dont use it
        current_descr = wd_item.get_description()
        # print(wd_item.fast_run_container.loaded_langs['en']['label'])
        self.descr = self.descr if self.descr else ''
        # print(wd_item.__dict__)
        # print(current_descr)
        # print(self.descr)
        # print(len(self.descr))
        if current_descr in {"", "human disease"}:
            if self.descr == "":
                wd_item.set_description(self.graph.DEFAULT_DESCRIPTION)
            elif len(self.descr) < 250:
                wd_item.set_description(utils.clean_description(self.descr))

    def set_label(self, wd_item):
        self.label = self.label.replace("(disease)", "").strip()
        super(MondoNode, self).set_label(wd_item)


class MondoGraph(Graph):
    NAME = "Monarch Disease Ontology"
    # GRAPH_URI = 'http://purl.obolibrary.org/obo/mondo/subsets/mondo-minimal.owl'
    GRAPH_URI = 'http://purl.obolibrary.org/obo/mondo.owl'
    QID = "Q27468140"
    DEFAULT_DESCRIPTION = "human disease"
    APPEND_PROPS = {PROPS['subclass of'], PROPS['instance of'],
                    PROPS['has cause'], PROPS['location'],
                    PROPS['OMIM ID'], PROPS['Orphanet ID'],
                    PROPS['MeSH ID'], PROPS['ICD-10-CM'],
                    PROPS['ICD-10'], PROPS['ICD-9-CM'],
                    PROPS['ICD-9'], PROPS['NCI Thesaurus ID'],
                    PROPS['UMLS CUI'], PROPS['Mondo ID'],
                    PROPS['GARD rare disease ID'], PROPS['Disease Ontology ID']}
    CORE_IDS = {
        PROPS['Disease Ontology ID'],
        PROPS['Mondo ID'],
        PROPS['MeSH ID'],
        PROPS['UMLS CUI'],
        PROPS['Orphanet ID'],
        PROPS['OMIM ID'],
        PROPS['GARD rare disease ID']
    }

    FAST_RUN = True

    PRED_PID_MAP = {'http://purl.obolibrary.org/obo/RO_0001025': PROPS['location'],
                    # 'http://purl.obolibrary.org/obo/RO_0002200': PROPS[],  # has phenotype
                    # 'http://purl.obolibrary.org/obo/IDO_0000664': PROPS[],  # has_material_basis_in
                    # 'http://purl.obolibrary.org/obo/RO_0003304': PROPS[],  # contributes to condition
                    # 'http://purl.obolibrary.org/obo/RO_0002451': PROPS[],  # transmitted by
                    # 'http://purl.obolibrary.org/obo/RO_0001020': PROPS[],  # is allergic trigger for
                    'is_a': PROPS['subclass of']}

    NODE_CLASS = MondoNode

    def filter_nodes(self):
        super(MondoGraph, self).filter_nodes()
        # self.nodes = self.nodes[:20]
        # self.nodes = [x for x in self.nodes if x.id_curie == "MONDO:0005393"]
        print("starting with {} nodes".format(len(self.nodes)))
        for node in self.nodes:
            # only take these xrefs
            prefixes = {'UMLS', 'Orphanet:', 'DOID:', 'OMIM:', 'MESH:', 'NCIT:', 'ICD10', 'ICD9', 'GARD:', 'HP:'}
            node.xrefs = set(x for x in node.xrefs if any(x.lower().startswith(prefix.lower()) for prefix in prefixes))
            if len(node.xrefs) == 0:
                node.id_uri = None
        self.nodes = [x for x in self.nodes if x.id_uri]
        print("after filtering branch {} nodes".format(len(self.nodes)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='run wikidata monarch disease ontology bot')
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

    g = MondoGraph(args.json_path, mediawiki_api_url=mediawiki_api_url, sparql_endpoint_url=sparql_endpoint_url)
    g.run(login)


"""
from obographs_MONDO import *
mediawiki_api_url = 'https://www.wikidata.org/w/api.php'
sparql_endpoint_url = 'https://query.wikidata.org/sparql'
g = MondoGraph("mondo.json", mediawiki_api_url=mediawiki_api_url, sparql_endpoint_url=sparql_endpoint_url)
g.FAST_RUN=False
g.nodes = g.nodes[1000:]
from scheduled_bots.local import WDUSER, WDPASS
login = wdi_login.WDLogin(WDUSER, WDPASS)
g.run(login)

"""
# next thing to do, get only nodes that have a DOID, then run them with CORE_ID being only DO
# example item that should get added: https://www.wikidata.org/wiki/Q5090449