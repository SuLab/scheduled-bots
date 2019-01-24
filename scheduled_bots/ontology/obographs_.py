"""
Generic ontology importer
"""

import argparse
import json
import os
import sys
import traceback
import urllib.request
from collections import defaultdict, Counter
from datetime import datetime
from itertools import chain
from time import gmtime, strftime, sleep

import requests
from tqdm import tqdm
from wikidataintegrator import wdi_core, wdi_helpers, wdi_login
from wikidataintegrator.wdi_helpers import id_mapper

from scheduled_bots.local import WDUSER, WDPASS

PROPS = {'subclass of': 'P279',
         'has cause': 'P828',
         'instance of': 'P31',
         'exact match': 'P2888',
         }


class Node:
    def __init__(self, node, graph):
        self.node = node
        self.graph = graph
        self.id_purl = node['id']
        self.id_colon = node['id'].split("/")[-1].replace("_", ":")
        self.primary_ext_prop_qid = graph.primary_ext_prop_qid
        self.default_label = graph.default_label
        self.release_qid = graph.release_qid
        self.fast_run = graph.fast_run
        self.parse_wikilinks = graph.parse_wikilinks


        self.lbl = node.get('lbl', None)
        self.type = node.get('type', None)
        self.namespace = None
        self.definition = None
        self.definition_xrefs = None
        self.deprecated = None
        self.alt_id = None
        self.synonym_xrefs = None
        self.synonym_values = None
        self.synonyms = None
        self.wikilink = None
        self.xrefs = []
        self.wd_item_id = None

        if 'meta' in node:
            self.parse_meta(node['meta'])
        self.relationships = []
        self.reference = None

    def parse_meta(self, meta):
        """
        Using: definition, deprecated, synonyms, basicPropertyValues
        :return:
        """
        self.definition = meta.get('definition', dict()).get('val', None)
        self.definition_xrefs = meta.get('definition', dict()).get('xrefs', None)
        self.deprecated = meta.get('deprecated', False)

        if 'xrefs' in meta:
            self.xrefs = [x['val'] for x in meta['xrefs']]

        if self.parse_wikilinks and self.definition_xrefs:
            url_xrefs = [x for x in self.definition_xrefs if 'url:http://en.wikipedia.org/wiki/' in x]
            if len(url_xrefs) > 1:
                print("{} multiple wikilinks: {}".format(self.id_purl, url_xrefs))
            elif len(url_xrefs) == 1:
                url = urllib.request.unquote(url_xrefs[0].replace("url:http://en.wikipedia.org/wiki/", ""))
                if '#' not in url:
                    # don't use links like 'Embryonal_carcinoma#Testicular_embryonal_carcinoma'
                    self.wikilink = url

        if 'basicPropertyValues' in meta:
            bp = defaultdict(set)
            for basicPropertyValue in meta['basicPropertyValues']:
                bp[basicPropertyValue['pred']].add(basicPropertyValue['val'])
            assert len(bp['http://www.geneontology.org/formats/oboInOwl#hasOBONamespace']) == 1
            self.namespace = list(bp['http://www.geneontology.org/formats/oboInOwl#hasOBONamespace'])[0]
            if 'http://www.geneontology.org/formats/oboInOwl#hasAlternativeId' in bp:
                self.alt_id = bp['http://www.geneontology.org/formats/oboInOwl#hasAlternativeId']

        if 'synonyms' in meta:
            sxref = defaultdict(set)
            sval = defaultdict(set)
            for syn in meta['synonyms']:
                sxref[syn['pred']].update(syn['xrefs'])
                sval[syn['pred']].add(syn['val'])
            self.synonym_xrefs = dict(sxref)
            self.synonym_values = dict(sval)
            self.synonyms = set(chain(*self.synonym_values.values())) - {self.lbl}

    def add_relationship(self, pred, obj):
        self.relationships.append((pred, obj))

    def create_item(self, login=None, write=True):
        if self.deprecated:
            return None
        try:
            s = []
            s.extend(self.create_xref_statements())
            s.extend(self.create_main_statements_nodepend())

            wd_item = wdi_core.WDItemEngine(data=s,
                                            append_value=[PROPS['subclass of'], PROPS['instance of']],
                                            fast_run=self.fast_run,
                                            fast_run_base_filter={self.primary_ext_prop_qid: ''})
            if wd_item.get_label(lang="en") == "":
                wd_item.set_label(self.lbl, lang="en")
            current_descr = wd_item.get_description(lang='en')
            if current_descr.lower() in {"", self.default_label} and self.definition and len(
                    self.definition) < 250:
                wd_item.set_description(description=self.definition, lang='en')
            elif current_descr.lower() == "":
                wd_item.set_description(description=self.default_label, lang='en')
            if self.synonyms is not None:
                wd_item.set_aliases(aliases=self.synonyms, lang='en', append=True)
            if self.wikilink is not None:
                wd_item.set_sitelink(site="enwiki", title=self.wikilink)
            wdi_helpers.try_write(wd_item, record_id=self.id_colon, record_prop=self.primary_ext_prop_qid, login=login,
                                  write=write)
            self.wd_item_id = wd_item.wd_item_id
            return wd_item
        except Exception as e:
            exc_info = sys.exc_info()
            traceback.print_exception(*exc_info)
            msg = wdi_helpers.format_msg(self.id_colon, self.primary_ext_prop_qid, None, str(e), msg_type=type(e))
            wdi_core.WDItemEngine.log("ERROR", msg)

    def create_depend(self, login=None, write=True):
        if self.deprecated:
            return None
        if not self.wd_item_id:
            print("must create item first: {}".format(node.id_purl))
            return None
        try:
            s = self.create_main_statements()
            wd_item = wdi_core.WDItemEngine(wd_item_id=self.wd_item_id, data=s,
                                            append_value=[PROPS['subclass of'], PROPS['instance of']],
                                            fast_run=self.fast_run,
                                            fast_run_base_filter={self.primary_ext_prop_qid: ''})
            wdi_helpers.try_write(wd_item, record_id=self.id_colon, record_prop=self.primary_ext_prop_qid, login=login,
                                  write=write)
            return wd_item
        except Exception as e:
            exc_info = sys.exc_info()
            traceback.print_exception(*exc_info)
            msg = wdi_helpers.format_msg(self.id_colon, self.primary_ext_prop_qid, None, str(e), msg_type=type(e))
            wdi_core.WDItemEngine.log("ERROR", msg)

    def create_reference(self):
        self.reference = Graph.create_ref_statement(release_qid=self.release_qid,
                                                    external_prop_id=self.primary_ext_prop_qid,
                                                    external_id=self.id_colon)

    def create_xref_statements(self):
        """
        These are string only and do not rely on any other items existing
        :return:
        """
        if not self.reference:
            self.create_reference()
        s = [wdi_core.WDExternalID(self.id_colon, self.primary_ext_prop_qid, references=[self.reference])]
        for xref in self.xrefs:
            prefix, code = xref.split(":", 1)
            if prefix in self.graph.xref_props:
                s.append(
                    wdi_core.WDExternalID(code, self.graph.xref_props[prefix], references=[self.reference]))
        return s

    def create_main_statements_nodepend(self):
        """
        Create statements that do not depend on any other node of this class
        For example, adding "instance of" "disease" to all diseases
        :return:
        """
        if not self.reference:
            self.create_reference()
        s = []

        # add http://purl.obolibrary.org/obo/, exact match
        s.append(wdi_core.WDString(self.id_purl, PROPS['exact match'], references=[self.reference]))

        return s

    def create_main_statements(self):
        """
        statememts that depends on the existance of another item in this class
        for example subclass of something in a heirarchy
        :return:
        """
        if not self.reference:
            self.create_reference()
        s = []
        for relationship in self.relationships:
            if relationship[0] not in self.graph.edge_props:
                print("unknown relationship: {}".format(relationship[0]))
                continue
                # todo log
            if relationship[1] not in self.graph.id_purl_qid:
                print("unknown obj: {}".format(relationship[1]))
                continue
                # todo log
            s.append(wdi_core.WDItemID(self.graph.id_purl_qid[relationship[1]],
                                       self.graph.edge_props[relationship[0]], references=[self.reference]))
        return s


class DONode(Node):
    def __init__(self, node, graph):
        super().__init__(node, graph)

    def create_main_statements_nodepend(self):
        s = super().create_main_statements_nodepend()
        if self.id_colon != "DOID:4":
            # instance of disease
            s.append(wdi_core.WDItemID('Q12136', PROPS['instance of'], references=[self.reference]))

        miriam_ref = [wdi_core.WDItemID(value="Q16335166", prop_nr='P248', is_reference=True),
                      wdi_core.WDUrl("http://www.ebi.ac.uk/miriam/main/collections/MIR:00000233", 'P854',
                                     is_reference=True)]
        s.append(
            wdi_core.WDString("http://identifiers.org/doid/{}".format(self.id_colon), PROPS['exact match'],
                              references=[miriam_ref]))
        return s


class Graph:
    def __init__(self, graph, ontology_qid, primary_ext_prop_qid, edge_props, xref_props, domain, default_label,
                 login=None, fast_run=True, parse_wikilinks=True, node_class=Node):
        """

        :param graph:
        :param ontology_qid: wdid for the ontology (e.g. Q5282129 for DO)
        :param primary_ext_prop_qid: property ID for the main external ID (e.g. P699 for DO)
        :param login:
        :param fast_run:
        """
        self.ontology_qid = ontology_qid
        self.primary_ext_prop_qid = primary_ext_prop_qid
        self.edge_props = edge_props
        self.xref_props = xref_props
        self.default_label = default_label
        self.login = login
        self.fast_run = fast_run
        self.parse_wikilinks = parse_wikilinks
        self.node_class = node_class

        self.version = None
        self.date = None
        self.default_namespace = None
        self.node_d = dict()
        self.release_qid = None
        self.main_id = None

        self.id_qid = None
        self.id_purl_qid = None

        self.parse_meta(graph['meta'])
        self.create_release()

        self.nodes = graph['nodes']
        self.parse_nodes(self.nodes)

        self.parse_edges(graph['edges'])
        if self.parse_wikilinks:
            self.dedupe_wikilinks()

    def get_id_map(self):
        self.id_qid = id_mapper(self.primary_ext_prop_qid)
        self.id_purl_qid = {"http://purl.obolibrary.org/obo/{}".format(k.replace(":", "_")): v for k, v in
                            self.id_qid.items()}

    def parse_meta(self, meta):
        self.version = meta['version']
        datestr = [x['val'] for x in meta['basicPropertyValues'] if
                   x['pred'] == 'http://www.geneontology.org/formats/oboInOwl#date'][0]
        self.date = datetime.strptime(datestr, '%m:%d:%Y %H:%M')
        self.default_namespace = [x['val'] for x in meta['basicPropertyValues'] if
                                  x['pred'] == 'http://www.geneontology.org/formats/oboInOwl#default-namespace'][0]

    def parse_nodes(self, nodes):
        if not self.release_qid:
            self.create_release()
        for node in nodes:
            tmp_node = self.node_class(node, self)
            if tmp_node.namespace == self.default_namespace and not tmp_node.deprecated and tmp_node.type == "CLASS":
                self.node_d[tmp_node.id_purl] = tmp_node
                #print(tmp_node.__dict__)
                #return

    def parse_edges(self, edges):
        for edge in edges:
            # don't add edges where the subject is a node not in this ontology
            if edge['sub'] not in self.node_d:
                continue
            self.node_d[edge['sub']].add_relationship(edge['pred'], edge['obj'])

    @staticmethod
    def get_item_label(qid):
        url = "https://www.wikidata.org/w/api.php?action=wbgetentities&ids={}&props=labels&languages=en&format=json".format(
            qid)
        r = requests.get(url)
        r.raise_for_status()
        d = r.json()
        if "error" in d:
            raise ValueError(d['error'])
        return d['entities'][qid]['labels']['en']['value']

    def create_release(self):
        #  get information about ontology to create/get release
        ontology_label = Graph.get_item_label(self.ontology_qid)
        print(ontology_label)

        r = wdi_helpers.Release('{} release {}'.format(ontology_label, self.date.strftime('%Y-%m-%d')),
                                'Release of {}'.format(ontology_label),
                                self.date.strftime('%Y-%m-%d'),
                                archive_url=self.version, edition_of_wdid=self.ontology_qid,
                                pub_date=self.date.date().strftime('+%Y-%m-%dT%H:%M:%SZ'))
        wd_item_id = r.get_or_create(self.login)
        if wd_item_id:
            self.release_qid = wd_item_id
        else:
            raise ValueError("unable to create release")

    def run(self, login=None, write=True):
        for node in self.node_d.values():
            node.create_item(login=login, write=write)
        print("waiting 5 min for endpoint to update")
        sleep(5*60)
        self.get_id_map()

        for node in self.node_d.values():
            node.create_depend(login=login, write=write)


    @staticmethod
    def create_ref_statement(release_qid, external_prop_id, external_id):
        stated_in = wdi_core.WDItemID(value=release_qid, prop_nr='P248', is_reference=True)
        ref_external_id = wdi_core.WDExternalID(value=external_id, prop_nr=external_prop_id, is_reference=True)
        ref_retrieved = wdi_core.WDTime(strftime("+%Y-%m-%dT00:00:00Z", gmtime()), prop_nr='P813', is_reference=True)
        do_reference = [stated_in, ref_retrieved, ref_external_id]
        return do_reference

    def dedupe_wikilinks(self):
        """remove sitelinks that are used for multiple nodes"""
        dupes = {k: v for k, v in Counter([x.wikilink for x in self.node_d.values() if x.wikilink]).items() if v > 1}
        for node in self.node_d.values():
            if node.wikilink in dupes:
                node.wikilink = None


if __name__ == "__main__":
    login = wdi_login.WDLogin(WDUSER, WDPASS)
    json_path = "/home/gstupp/projects/data/doid.json"
    d = json.load(open(json_path))
    graph = d['graphs'][0]

    edge_props = {'is_a': 'P279'}  # "subclass of"

    xref_props = {'ORDO': 'P1550',
                  'UMLS_CUI': 'P2892',
                  'DOID': 'P699',
                  'ICD10CM': 'P494',
                  'ICD9CM': 'P493',
                  'MSH': 'P486',
                  'NCI': 'P1748',
                  'OMIM': 'P492'}

    do_graph = Graph(graph, "Q5282129", "P699", edge_props, xref_props, "disease", "disease", login=login, fast_run=False,
                     parse_wikilinks=False, node_class=DONode)

    node = do_graph.node_d['http://purl.obolibrary.org/obo/DOID_8717']
    node.create_item(login=login, write=False)
    node.create_depend(login)