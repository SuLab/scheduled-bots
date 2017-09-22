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

from wikidataintegrator import wdi_core, wdi_helpers, wdi_login, wdi_property_store
from wikidataintegrator.ref_handlers import update_retrieved_if_new
from wikidataintegrator.wdi_helpers import id_mapper

try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

PROPS = {'subclass of': 'P279',
         'has cause': 'P828',
         'instance of': 'P31',
         'exact match': 'P2888',
         'Orphanet ID': 'P1550',
         'UMLS CUI': 'P2892',
         'Disease Ontology ID': 'P699',
         'ICD-10': 'P494',
         'ICD-10-CM': 'P4229',
         'ICD-9': 'P493',
         'ICD-9-CM': 'P1692',
         'MeSH ID': 'P486',
         'NCI Thesaurus ID': 'P1748',
         'OMIM ID': 'P492',
         'location': 'P276'
         }

wdi_property_store.wd_properties[PROPS['OMIM ID']]['core_id'] = False
wdi_property_store.wd_properties[PROPS['MeSH ID']]['core_id'] = False

__metadata__ = {'name': 'DOIDBot',
                'tags': ['disease', 'doid'],
                'properties': list(PROPS.values())
                }


class DOGraph:
    edge_prop = {
        # 'http://purl.obolibrary.org/obo/IDO_0000664': 'P828',  # has_material_basis_in -> has cause
        'http://purl.obolibrary.org/obo/RO_0001025': 'P276',  # located in
        # 'http://purl.obolibrary.org/obo/RO_0002451': None,  # transmitted by. "pathogen transmission process" (P1060)?
        'is_a': 'P279'}

    xref_prop = {'ORDO': 'P1550',
                 'UMLS_CUI': 'P2892',
                 'DOID': 'P699',
                 'ICD10CM': 'P4229',
                 'ICD10': 'P494',
                 'ICD9CM': 'P1692',
                 'ICD9': 'P493',
                 'MSH': 'P486',
                 'MESH': 'P486',
                 'NCI': 'P1748',
                 'OMIM': 'P492',
                 # 'SNOMEDCT_US_2016_03_01': ''  # can't put in wikidata...
                 }
    purl_wdid = dict()

    def get_existing_items(self):
        # get all existing items we can add relationships to
        doid_wdid = id_mapper('P699')
        self.purl_wdid = {"http://purl.obolibrary.org/obo/{}".format(k.replace(":", "_")): v for k, v in
                          doid_wdid.items()}
        # add in uberon items so we can do "located in"
        uberon_wdid = id_mapper('P1554')
        self.purl_wdid.update({"http://purl.obolibrary.org/obo/UBERON_{}".format(k): v for k, v in uberon_wdid.items()})
        # add in taxonomy items for "has_material_basis_in"
        ncbi_wdid = id_mapper("P685")
        self.purl_wdid.update(
            {"http://purl.obolibrary.org/obo/NCBITaxon_{}".format(k): v for k, v in ncbi_wdid.items()})

    def __init__(self, graph, login=None, fast_run=True):
        self.fast_run = fast_run
        self.version = None
        self.date = None
        self.default_namespace = None
        self.login = login
        self.nodes = dict()
        self.parse_meta(graph['meta'])
        self.parse_nodes(graph['nodes'])
        self.parse_edges(graph['edges'])
        self.dedupe_wikilinks()
        self.release = None
        if not self.purl_wdid:
            self.get_existing_items()

    def parse_meta(self, meta):
        self.version = meta['version']
        datestr = [x['val'] for x in meta['basicPropertyValues'] if
                   x['pred'] == 'http://www.geneontology.org/formats/oboInOwl#date'][0]
        self.date = datetime.strptime(datestr, '%d:%m:%Y %H:%M')
        self.default_namespace = [x['val'] for x in meta['basicPropertyValues'] if
                                  x['pred'] == 'http://www.geneontology.org/formats/oboInOwl#default-namespace'][0]

    def parse_nodes(self, nodes):
        for node in nodes:
            tmp_node = DONode(node, self)
            if "http://purl.obolibrary.org/obo/DOID_" in tmp_node.id and not tmp_node.deprecated and tmp_node.type == "CLASS":
                self.nodes[tmp_node.id] = tmp_node

    def parse_edges(self, edges):
        for edge in edges:
            # don't add edges where the subject is a node not in this ontology
            if edge['sub'] not in self.nodes:
                continue
            self.nodes[edge['sub']].add_relationship(edge['pred'], edge['obj'])

    def create_release(self):
        r = wdi_helpers.Release('Disease Ontology release {}'.format(self.date.strftime('%Y-%m-%d')),
                                'Release of the Disease Ontology', self.date.strftime('%Y-%m-%d'),
                                archive_url=self.version, edition_of_wdid='Q5282129',
                                pub_date=self.date.date().strftime('+%Y-%m-%dT%H:%M:%SZ'))
        wd_item_id = r.get_or_create(self.login)
        if wd_item_id:
            self.release = wd_item_id
        else:
            raise ValueError("unable to create release")

    def create_ref_statement(self, doid):
        if not self.release:
            self.create_release()
        stated_in = wdi_core.WDItemID(value=self.release, prop_nr='P248', is_reference=True)
        ref_doid = wdi_core.WDExternalID(value=doid, prop_nr='P699', is_reference=True)
        ref_retrieved = wdi_core.WDTime(strftime("+%Y-%m-%dT00:00:00Z", gmtime()), prop_nr='P813', is_reference=True)
        do_reference = [stated_in, ref_retrieved, ref_doid]
        return do_reference

    def dedupe_wikilinks(self):
        """remove sitelinks that are used for multiple nodes"""
        dupes = {k: v for k, v in Counter([x.wikilink for x in self.nodes.values() if x.wikilink]).items() if v > 1}
        for node in self.nodes.values():
            if node.wikilink in dupes:
                node.wikilink = None


class DONode:
    def __init__(self, node, do_graph):
        self.do_graph = do_graph
        self.id = node['id']
        self.doid = node['id'].split("/")[-1].replace("_", ":")
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
        if 'meta' in node:
            self.parse_meta(node['meta'])
        self.relationships = []
        self.reference = None

        self.s = []  # statements
        self.s_xref = None
        self.s_main = None

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

        if self.definition_xrefs:
            url_xrefs = [x for x in self.definition_xrefs if 'url:http://en.wikipedia.org/wiki/' in x]
            if len(url_xrefs) == 1:
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

    def get_dependencies(self, relationships):
        """
        What wikidata IDs do we need to have before we can make this item?
        :return:
        """
        # todo: This is not implemented
        need_purl = [x[1] for x in self.relationships if x[0] in relationships]
        return [x for x in need_purl if x not in self.do_graph.purl_wdid]

    def create(self, write=True):
        if self.deprecated:
            msg = wdi_helpers.format_msg(self.doid, 'P699', None, "delete me", msg_type="delete me")
            wdi_core.WDItemEngine.log("WARNING", msg)
            print(msg)
            return None
        try:
            self.create_xref_statements()
            self.s.extend(self.s_xref)
            self.create_main_statements()
            self.s.extend(self.s_main)
            wd_item = wdi_core.WDItemEngine(item_name=self.lbl, data=self.s, domain="diseases",
                                            append_value=[PROPS['subclass of'], PROPS['instance of'],
                                                          PROPS['has cause'], PROPS['location'],
                                                          PROPS['OMIM ID']],
                                            fast_run=self.do_graph.fast_run,
                                            fast_run_base_filter={'P699': ''},
                                            fast_run_use_refs=True,
                                            global_ref_mode='CUSTOM',
                                            ref_handler=update_retrieved_if_new
                                            )
            wd_item.fast_run_container.debug = False
            if wd_item.get_label(lang="en") == "":
                wd_item.set_label(self.lbl, lang="en")
            current_descr = wd_item.get_description(lang='en')
            if current_descr.lower() in {"", "human disease", "disease"} and self.definition and len(
                    self.definition) < 250:
                wd_item.set_description(description=self.definition, lang='en')
            elif current_descr.lower() == "":
                wd_item.set_description(description="human disease", lang='en')
            if self.synonyms is not None:
                wd_item.set_aliases(aliases=self.synonyms, lang='en', append=True)
            if self.wikilink is not None:
                # a lot of these are not right... don't do this
                # wd_item.set_sitelink(site="enwiki", title=self.wikilink)
                pass
            wdi_helpers.try_write(wd_item, record_id=self.doid, record_prop='P699', login=self.do_graph.login,
                                  write=write)
            return wd_item
        except Exception as e:
            exc_info = sys.exc_info()
            print(self.doid)
            traceback.print_exception(*exc_info)
            msg = wdi_helpers.format_msg(self.doid, 'P699', None, str(e), msg_type=type(e))
            wdi_core.WDItemEngine.log("ERROR", msg)

    def create_reference(self):
        self.reference = self.do_graph.create_ref_statement(self.doid)

    def create_xref_statements(self):
        if not self.reference:
            self.create_reference()
        self.s_xref = []
        self.s_xref.append(wdi_core.WDExternalID(self.doid, PROPS['Disease Ontology ID'], references=[self.reference]))
        for xref in self.xrefs:
            if ":" in xref:
                prefix, code = xref.split(":", 1)
                if prefix.upper() in DOGraph.xref_prop:
                    if prefix.upper() == "OMIM" and code.startswith("PS"):
                        continue
                    self.s_xref.append(
                        wdi_core.WDExternalID(code, DOGraph.xref_prop[prefix.upper()], references=[self.reference]))

    def create_main_statements(self):
        if not self.reference:
            self.create_reference()
        self.s_main = []
        for relationship in self.relationships:
            if relationship[0] not in self.do_graph.edge_prop:
                # s = "unknown relationship: {}".format(relationship[0])
                # msg = wdi_helpers.format_msg(self.doid, 'P699', None, s, msg_type="unknown relationship")
                # wdi_core.WDItemEngine.log("WARNING", msg)
                continue
            if relationship[1] not in self.do_graph.purl_wdid:
                s = "unknown obj: {}".format(relationship[1])
                msg = wdi_helpers.format_msg(self.doid, 'P699', None, s, msg_type="unknown obj")
                wdi_core.WDItemEngine.log("WARNING", msg)
                continue
            self.s_main.append(wdi_core.WDItemID(self.do_graph.purl_wdid[relationship[1]],
                                                 self.do_graph.edge_prop[relationship[0]], references=[self.reference]))
        # add http://purl.obolibrary.org/obo/, exact match
        self.s_main.append(wdi_core.WDString(self.id, PROPS['exact match'], references=[self.reference]))

        if self.doid != "DOID:4":
            # instance of disease
            self.s_main.append(wdi_core.WDItemID('Q12136', PROPS['instance of'], references=[self.reference]))

        miriam_ref = [wdi_core.WDItemID(value="Q16335166", prop_nr='P248', is_reference=True),
                      wdi_core.WDUrl("http://www.ebi.ac.uk/miriam/main/collections/MIR:00000233", 'P854',
                                     is_reference=True)]
        self.s_main.append(wdi_core.WDString("http://identifiers.org/doid/{}".format(self.doid), PROPS['exact match'],
                                             references=[miriam_ref]))


def get_releases():
    s = "select ?item where {?item wdt:P856 <http://disease-ontology.org> .}"
    return [x['item']['value'].split("/")[-1] for x in
            wdi_core.WDItemEngine.execute_sparql_query(s)['results']['bindings']]


RELEASES = get_releases()


def remove_deprecated_statements(qid, frc, release_wdid, props, login):
    releases = set(int(x.replace("Q", "")) for x in RELEASES)
    # don't count this release
    releases.discard(int(release_wdid.replace("Q", "")))

    for prop in props:
        frc.write_required([wdi_core.WDString("fake value", prop)])
    orig_statements = frc.reconstruct_statements(qid)

    s_dep = []
    for s in orig_statements:
        if any(any(x.get_prop_nr() == 'P248' and x.get_value() in releases for x in r) and any(
                        x.get_prop_nr() == 'P1065' for x in r) for r in s.get_references()):
            setattr(s, 'remove', '')
            s_dep.append(s)

    if s_dep:
        print("-----")
        print(qid)
        print(len(s_dep))
        print([(x.get_prop_nr(), x.value) for x in s_dep])
        print([(x.get_references()[0]) for x in s_dep])
        wd_item = wdi_core.WDItemEngine(wd_item_id=qid, domain='none', data=s_dep, fast_run=False)
        wdi_helpers.try_write(wd_item, '', '', login, edit_summary="remove deprecated statements")


def main(json_path='doid.json', log_dir="./logs", fast_run=True, write=True):
    login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS)
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, logger_name='WD_logger', log_name=log_name,
                                        header=json.dumps(__metadata__))

    with open(json_path) as f:
        d = json.load(f)
    graphs = {g['id']: g for g in d['graphs']}
    graph = graphs['http://purl.obolibrary.org/obo/doid.owl']
    # get the has phenotype, has_material_basis_in, and transmitted by edges from another graph
    graph['edges'].extend(graphs['http://purl.obolibrary.org/obo/doid/obo/ext.owl']['edges'])
    do = DOGraph(graph, login, fast_run)
    nodes = sorted(do.nodes.values(), key=lambda x: x.doid)
    items = []
    for n, node in tqdm(enumerate(nodes), total=len(nodes)):
        item = node.create(write=write)
        #if n>100:
        #    sys.exit(0)
        if item:
            items.append(item)

    sleep(10 * 60)
    doid_wdid = id_mapper('P699')
    frc = items[0].fast_run_container
    if not frc:
        print("fastrun container not found. not removing deprecated statements")
        return None
    frc.clear()
    for doid in tqdm(doid_wdid.values()):
        remove_deprecated_statements(doid, frc, do.release, list(PROPS.values()), login)


if __name__ == "__main__":
    """
    Bot to add/update disease ontology to wikidata. Requires an obograph-generated json file.
    See: https://github.com/geneontology/obographs
    """
    parser = argparse.ArgumentParser(description='run wikidata disease ontology bot')
    parser.add_argument("json_path", help="Path to json file")
    parser.add_argument('--log-dir', help='directory to store logs', type=str)
    parser.add_argument('--dummy', help='do not actually do write', action='store_true')
    parser.add_argument('--fastrun', dest='fastrun', action='store_true')
    parser.add_argument('--no-fastrun', dest='fastrun', action='store_false')
    parser.set_defaults(fastrun=True)
    args = parser.parse_args()
    log_dir = args.log_dir if args.log_dir else "./logs"
    run_id = datetime.now().strftime('%Y%m%d_%H:%M')
    __metadata__['run_id'] = run_id
    fast_run = args.fastrun

    log_name = '{}-{}.log'.format(__metadata__['name'], run_id)
    if wdi_core.WDItemEngine.logger is not None:
        wdi_core.WDItemEngine.logger.handles = []
    wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(__metadata__),
                                        logger_name='doid')

    if args.json_path.startswith("http"):
        print("Downloading")
        response = requests.get(args.json_path)
        with open('/tmp/do.json', 'wb') as f:
            f.write(response.content)
        args.json_path = '/tmp/do.json'
        print("Done downloading")

    main(args.json_path, log_dir=log_dir, fast_run=fast_run, write=not args.dummy)
