"""
This is a generic OWL -> Wikidata importer
Requires a obographs JSON file made from an owl file

"""
import json
import multiprocessing
import os
import subprocess
import traceback
from collections import defaultdict
from datetime import datetime
from functools import partial
from itertools import chain
from time import strftime, gmtime

import networkx as nx
import requests
from tqdm import tqdm
from wikicurie.wikicurie import CurieUtil

from scheduled_bots import utils
from wikidataintegrator import wdi_core, wdi_helpers
from wikidataintegrator.ref_handlers import update_release
from wikidataintegrator.wdi_helpers import WikibaseHelper

cu = CurieUtil()


class Node:
    def __init__(self, json_node, graph):
        self.json_node = json_node
        self.id_uri = json_node['id']  # e.g. http://purl.obolibrary.org/obo/DOID_8718
        try:
            self.id_curie = cu.uri_to_curie(self.id_uri)
        except AssertionError:
            self.id_curie = None
        try:
            # the wikidata property id for this node's id, and the value to be used in wikidata
            self.id_pid, self.id_value = cu.parse_curie(self.id_curie)
            self.id_pid = graph.helper.get_pid(self.id_pid)
        except Exception:
            self.id_pid = None
            self.id_value = None
        self.label = json_node.get("lbl")
        self.type = json_node.get("type")
        self.graph = graph
        self.helper = graph.helper
        self.mediawiki_api_url = graph.mediawiki_api_url
        self.sparql_endpoint_url = graph.sparql_endpoint_url
        self.ref_handler = graph.ref_handler
        self.pids = set()  # set of pids this node used

        self.qid = None
        self.xrefs = set()
        self.item = None
        self.bpv = defaultdict(set)

        self.descr = ''
        self.deprecated = None
        self.synonyms = set()
        self.qid = None

        self.parse_meta()

    def parse_meta(self):
        meta = self.json_node.get("meta", dict())
        if 'basicPropertyValues' in meta:
            for basicPropertyValue in meta['basicPropertyValues']:
                self.bpv[basicPropertyValue['pred']].add(basicPropertyValue['val'])
        if 'xrefs' in meta:
            self.xrefs = set([x['val'] for x in meta['xrefs']])
        self.descr = meta.get('definition', dict()).get('val')
        self.deprecated = meta.get('deprecated', False)
        self.synonyms = set(x['val'] for x in meta.get('synonyms', list()))
        # filter out the label from the synonyms
        self.synonyms.discard(self.label)

    def create_ref_statement(self):
        assert self.graph.release_qid, "create the release first (on the graph class)"

        stated_in = wdi_core.WDItemID(value=self.graph.release_qid, prop_nr=self.helper.get_pid('P248'),
                                      is_reference=True)
        ref_extid = wdi_core.WDExternalID(value=self.id_value, prop_nr=self.id_pid, is_reference=True)
        ref_retrieved = wdi_core.WDTime(strftime("+%Y-%m-%dT00:00:00Z", gmtime()),
                                        prop_nr=self.helper.get_pid('P813'), is_reference=True)
        reference = [stated_in, ref_retrieved, ref_extid]
        return reference

    def set_label(self, wd_item):
        # only setting the label if its currently blank or a new item is being created
        if not wd_item.get_label():
            wd_item.set_label(self.label)

    def set_descr(self, wd_item):
        # if the current description is blank and the new description
        # is something else (and not over 250 characters), use it
        current_descr = wd_item.get_description()
        if (not current_descr) and self.descr and len(self.descr) < 250:
            wd_item.set_description(utils.clean_description(self.descr))
        elif not current_descr:
            wd_item.set_description(self.graph.DEFAULT_DESCRIPTION)

    def set_aliases(self, wd_item):
        if self.synonyms is not None:
            synonyms = {x for x in self.synonyms if x.lower() != self.label.lower()}
            wd_item.set_aliases(aliases=synonyms, append=True)

    def create_statements(self):
        ref = self.create_ref_statement()
        self.pids.add(self.id_pid)

        # make sure this ID is unique in wikidata
        self.graph.CORE_IDS.update({self.id_pid})
        # this node's primary id
        s = [wdi_core.WDExternalID(self.id_value, self.id_pid, references=[ref])]
        # add the exact match statements
        s.append(wdi_core.WDUrl(self.id_uri, self.helper.get_pid('P2888'), references=[ref]))

        s.extend(self.create_xref_statements())

        return s

    def create_xref_statements(self):
        ss = []
        for xref in self.xrefs:
            s = self.create_xref_statement(xref)
            if s:
                ss.append(s)
        return ss

    def create_xref_statement(self, xref):
        ref = self.create_ref_statement()
        if xref.split(":")[0] not in cu.curie_map:
            # log this curie prefix not being found
            m = wdi_helpers.format_msg(self.id_curie, self.id_pid, self.qid,
                                       "curie prefix not found: {}".format(xref.split(":")[0]))
            wdi_core.WDItemEngine.log("WARNING", m)
            return None
        pid, ext_id = cu.parse_curie(xref)
        pid = self.helper.get_pid(pid)
        self.pids.add(pid)
        return wdi_core.WDExternalID(ext_id, pid, references=[ref])

    def _pre_create(self):
        # override in subclass to do something before the node is created
        pass

    def create(self, login, write=True, allow_new=True):
        # create or get qid
        # creates the primary external ID, the xrefs, instance of (if set), checks label, description, and aliases
        # not other properties (i.e. subclass), as these may require items existing that may not exist yet
        self._pre_create()
        assert self.id_curie
        s = self.create_statements()

        primary_ext_id_pid, primary_ext_id = cu.parse_curie(self.id_curie)
        primary_ext_id_pid = self.helper.get_pid(primary_ext_id_pid)
        assert primary_ext_id_pid in self.graph.APPEND_PROPS

        try:
            self.item = wdi_core.WDItemEngine(
                data=s,
                append_value=self.graph.APPEND_PROPS,
                fast_run=self.graph.FAST_RUN,
                fast_run_base_filter={primary_ext_id_pid: ''},
                fast_run_use_refs=True,
                global_ref_mode='CUSTOM',
                ref_handler=self.ref_handler,
                mediawiki_api_url=self.mediawiki_api_url,
                sparql_endpoint_url=self.sparql_endpoint_url,
                core_props=self.graph.CORE_IDS,
                core_prop_match_thresh=.9
            )
            # assert the retrieved item doesn't already have a primary_ext_id id
            if self.item.wd_item_id:
                query = "select ?primary_ext_id where {{ wd:{} wdt:{} ?primary_ext_id }}".format(self.item.wd_item_id,
                                                                                                 primary_ext_id_pid)
                results = wdi_core.WDItemEngine.execute_sparql_query(query)['results']['bindings']
                if results:
                    existing_primary_ext_id = [x['primary_ext_id']['value'] for x in results]
                    if self.id_curie not in existing_primary_ext_id:
                        raise Exception(
                            "conflicting primary_ext_id IDs: {} on {}".format(self.id_curie, self.item.wd_item_id))
            if self.item.create_new_item and not allow_new:
                return None
        except Exception as e:
            traceback.print_exc()
            msg = wdi_helpers.format_msg(primary_ext_id, primary_ext_id_pid, None, str(e), msg_type=type(e))
            wdi_core.WDItemEngine.log("ERROR", msg)
            return
        self.set_label(self.item)
        self.set_descr(self.item)
        self.set_aliases(self.item)
        # todo: I want to avoid this from happening: https://www.wikidata.org/w/index.php?title=Q4553565&diff=676750840&oldid=647941942

        wdi_helpers.try_write(self.item, record_id=primary_ext_id, record_prop=primary_ext_id_pid,
                              login=login, write=False)

        self.qid = self.item.wd_item_id

    def remove_deprecated_statements(self, releases, frc, login):
        """

        :param releases: a set of qid for releases which, when used as 'stated in' on a reference,
        the statement should be removed
        :param frc:
        :param login:
        :return:
        """

        def is_old_ref(ref, releases):
            stated_in = self.helper.get_pid('P248')
            return any(r.get_prop_nr() == stated_in and "Q" + str(r.get_value()) in releases for r in ref)

        qid = self.qid
        if qid is None:
            return None
        primary_ext_id_pid, primary_ext_id = cu.parse_curie(self.id_curie)
        primary_ext_id_pid = self.helper.get_pid(primary_ext_id_pid)

        if frc is None:
            statements = wdi_core.WDItemEngine(wd_item_id=self.qid).statements
        else:
            statements = frc.reconstruct_statements(qid)

        s_remove = []
        s_deprecate = []
        for s in statements:
            if len(s.get_references()) == 1 and is_old_ref(s.get_references()[0], releases):
                # this is the only ref on this statement and its from an old release
                if s.get_prop_nr() == primary_ext_id_pid:
                    # if its on the primary ID for this item, deprecate instead of removing it
                    s.set_rank('deprecated')
                    s_deprecate.append(s)
                else:
                    setattr(s, 'remove', '')
                    s_remove.append(s)
            if len(s.get_references()) > 1 and any(is_old_ref(ref, releases) for ref in s.get_references()):
                # there is another reference on this statement, and a old reference
                # we should just remove the old reference and keep the statement
                s.set_references([ref for ref in s.get_references() if not is_old_ref(ref, releases)])
                s_deprecate.append(s)

        if s_deprecate or s_remove:
            print("-----")
            print(qid)
            print([(x.get_prop_nr(), x.value) for x in s_deprecate])
            print([(x.get_prop_nr(), x.value) for x in s_remove])
            """
            I don't know why I have to split it up like this, but if you try to remove statements with append_value
            set, the statements don't get removed, and if you try to remove a ref off a statement without append_value
            set, then all other statements get removed. It works if you do them seperately...
            """
            if s_deprecate:
                wd_item = wdi_core.WDItemEngine(wd_item_id=qid, data=s_deprecate, fast_run=False,
                                                mediawiki_api_url=self.mediawiki_api_url,
                                                sparql_endpoint_url=self.sparql_endpoint_url,
                                                append_value=self.graph.APPEND_PROPS)
                wdi_helpers.try_write(wd_item, '', '', login, edit_summary="remove deprecated statements")
            if s_remove:
                wd_item = wdi_core.WDItemEngine(wd_item_id=qid, data=s_remove, fast_run=False,
                                                mediawiki_api_url=self.mediawiki_api_url,
                                                sparql_endpoint_url=self.sparql_endpoint_url)
                wdi_helpers.try_write(wd_item, '', '', login, edit_summary="remove deprecated statements")


def create_node(node, login, write, create_new):
    # pickleable function that calls create on node
    node.create(login, write, create_new)


class Graph:
    # the following MUST be overridden in a subclass !!!
    NAME = None
    QID = None
    GRAPH_URI = None
    DEFAULT_DESCRIPTION = None
    APPEND_PROPS = None
    FAST_RUN = None
    PRED_PID_MAP = None

    # the following must be overriden if a custom node type is being used
    NODE_CLASS = Node

    # the following is optional
    EXCLUDE_NODES = set()
    CORE_IDS = set()

    def __init__(self, json_path, mediawiki_api_url='https://www.wikidata.org/w/api.php',
                 sparql_endpoint_url='https://query.wikidata.org/sparql'):
        assert self.NAME, "Must initialize subclass"
        self.json_path = self.handle_file(json_path)
        self.mediawiki_api_url = mediawiki_api_url
        self.sparql_endpoint_url = sparql_endpoint_url

        self.version = None
        self.edition = None
        self.date = None
        self.json_graph = None
        self.nodes = None
        self.deprecated_nodes = None
        self.edges = None
        self.release = None  # the wdi_helper.Release instance
        self.root_node = None
        self.G = None  # a networkx directed graph using is_a relationships. built in calculate_root_nodes

        # str: the QID of the release item. e.g.:
        self.release_qid = None
        # dict[str, dict[str, str]]: use for mapping URIs from external ontologies. e.g. {'UBERON': {'1234': 'Q5453'}}
        self.pid_id_mapper = dict()
        # URIs from this owl file mapping to the node python object
        self.uri_node_map = dict()
        # we want to save all of the properties we used
        self.pids = set()

        self.ref_handler = None
        self.helper = WikibaseHelper(sparql_endpoint_url)

        # get the localized version of these pids and qids
        self.QID = self.helper.get_qid(self.QID)
        self.PRED_PID_MAP = {k: self.helper.get_pid(v) if v else None for k, v in self.PRED_PID_MAP.items()}
        self.APPEND_PROPS = list({self.helper.get_pid(v) for v in self.APPEND_PROPS})
        self.APPEND_PROPS.append(self.helper.get_pid('P2888'))  # exact match

        self.load_graph()
        self.parse_graph()

    def run(self, login):
        self.setup_logging()
        self.create_release(login)
        self.set_ref_handler()
        self.create_nodes(login)
        self.create_edges(login)
        last_edit_time = datetime.utcnow()
        self.check_for_existing_deprecated_nodes()
        entity = "http://www.wikidata.org" if self.sparql_endpoint_url == 'https://query.wikidata.org/sparql' else 'http://wikibase.svc'
        wdi_helpers.wait_for_last_modified(last_edit_time, endpoint=self.sparql_endpoint_url, entity=entity)
        self.remove_deprecated_statements(login)

    def load_graph(self):
        with open(self.json_path) as f:
            d = json.load(f)
        graphs = {g['id']: g for g in d['graphs']}
        if self.GRAPH_URI not in graphs:
            raise ValueError("{} not found. Available graphs: {}".format(self.GRAPH_URI, list(graphs.keys())))
        self.json_graph = graphs[self.GRAPH_URI]

    def set_ref_handler(self):
        self.ref_handler = partial(update_release, retrieved_pid=self.helper.get_pid("P813"),
                                   stated_in_pid=self.helper.get_pid("P248"),
                                   old_stated_in=self._get_old_releases())
        for node in self.nodes:
            node.ref_handler = self.ref_handler

    @staticmethod
    def generate_obograph_from_owl(owl_file_path):
        # assumes `ogger` is in your path
        # See: https://github.com/geneontology/obographs
        json_file_path = owl_file_path.replace(".owl", ".json")
        print("generating obographs file: {}".format(json_file_path))
        with open(json_file_path, 'w') as f:
            c = subprocess.check_call(['ogger', owl_file_path], stdout=f)
        if c == 0:
            return json_file_path

    @staticmethod
    def download_file(file_path):
        file_name = os.path.split(file_path)[1]
        print("Downloading")
        response = requests.get(file_path)
        download_path = os.path.join('/tmp/', file_name)
        with open(download_path, 'wb') as f:
            f.write(response.content)
        print("Done downloading: {}".format(download_path))
        return download_path

    @staticmethod
    def handle_file(file_path):
        # is this a URL?
        if file_path.startswith("http") or file_path.startswith("ftp"):
            file_path = Graph.download_file(file_path)

        # if its an owl file, try to convert
        if file_path.endswith(".owl"):
            file_path = Graph.generate_obograph_from_owl(file_path)

        return file_path

    def setup_logging(self, log_dir="./logs"):
        metadata = {
            'name': self.NAME,
            'run_id': datetime.now().strftime('%Y%m%d_%H:%M')
        }
        log_name = '{}-{}.log'.format(metadata['name'], metadata['run_id'])
        if wdi_core.WDItemEngine.logger is not None:
            wdi_core.WDItemEngine.logger.handles = []
        wdi_core.WDItemEngine.setup_logging(log_dir=log_dir, log_name=log_name, header=json.dumps(metadata),
                                            logger_name=metadata['name'])

    def parse_graph(self):
        self.parse_meta()
        self.parse_nodes()
        self.filter_nodes()
        self.parse_edges()
        self.filter_edges()
        self.calculate_root_nodes()
        self.nodes = sorted(self.nodes, key=lambda x: x.id_uri)
        self._post_parse_graph()

    def _post_parse_graph(self):
        # this gets called after self.parse_graph
        pass

    def parse_meta(self):
        meta = self.json_graph['meta']

        # this is a PURI to the release owl file
        # e.g. : http://purl.obolibrary.org/obo/doid/releases/2018-03-02/doid.owl
        self.version = meta['version']
        # convert the version string to a version string to use for the release item in WD
        self.edition = self.version.rsplit("/", 2)[1]
        # date is for the release item as well
        self.date = datetime.strptime(self.edition, '%Y-%m-%d')

    def parse_nodes(self):
        self.nodes = [self.NODE_CLASS(json_node, self) for json_node in
                      self.json_graph['nodes']]
        self.uri_node_map = {node.id_uri: node for node in self.nodes}

    def parse_edges(self):
        self.edges = self.json_graph['edges']
        # list of dicts, looks like
        # [{'sub': 'http://purl.obolibrary.org/obo/DOID_820',
        # 'pred': 'http://purl.obolibrary.org/obo/RO_0001025',
        # 'obj': 'http://purl.obolibrary.org/obo/UBERON_0000948'}, ...]

    def filter_edges(self):
        # keep ony the edges where the subject is in the list of filtered nodes
        node_uris = {node.id_uri for node in self.nodes}
        self.edges = [edge for edge in self.edges if edge['sub'] in node_uris]
        # and the pred is in the predicate to PID map "PROP_PID_MAP"
        self.edges = [edge for edge in self.edges if edge['pred'] in self.PRED_PID_MAP]

    def filter_nodes(self):
        self.deprecated_nodes = [x for x in self.nodes if x.deprecated and x.type == "CLASS"]
        self.nodes = [x for x in self.nodes if not x.deprecated and x.type == "CLASS"]
        self.nodes = [x for x in self.nodes if x.id_uri not in self.EXCLUDE_NODES]
        self.nodes = [x for x in self.nodes if x.id_pid]
        self.nodes = [x for x in self.nodes if x.label]

    def create_nodes_par(self, login, write=True):
        pool = multiprocessing.Pool(processes=4)
        create_node_f = partial(create_node, login=login, write=write)
        for _ in tqdm(pool.imap(create_node_f, self.nodes, chunksize=1000), total=len(self.nodes),
                      desc="creating items"):
            pass

    def create_nodes(self, login, write=True, create_new=True):
        create_node_f = partial(create_node, login=login, write=write, create_new=create_new)
        for _ in tqdm(map(create_node_f, self.nodes), total=len(self.nodes), desc="creating items"):
            pass

    def create_release(self, login):
        self.release = wdi_helpers.Release('{} release {}'.format(self.NAME, self.edition),
                                           'Release of the {}'.format(self.NAME), self.edition,
                                           archive_url=self.version, edition_of_wdid=self.QID,
                                           pub_date=self.date.date().strftime('+%Y-%m-%dT%H:%M:%SZ'),
                                           sparql_endpoint_url=self.sparql_endpoint_url,
                                           mediawiki_api_url=self.mediawiki_api_url)
        wd_item_id = self.release.get_or_create(login)
        if wd_item_id:
            self.release_qid = wd_item_id
        else:
            raise ValueError("unable to create release")

    def create_edges(self, login, write=True):

        # skip edges where the subject is not one of our nodes
        all_uris = set(node.id_uri for node in self.nodes)
        skipped_edges = [e for e in self.edges if e['sub'] not in all_uris]
        print("skipping {} edges where the subject is a node that is being skipped".format(len(skipped_edges)))

        for node in tqdm(self.nodes, desc="creating edges"):
            if not node.qid:
                m = wdi_helpers.format_msg(node.id_curie, node.id_pid, None, "QID not found, skipping edges")
                print(m)
                wdi_core.WDItemEngine.log("WARNING", m)
                continue
            this_uri = node.id_uri
            this_edges = [edge for edge in self.edges if edge['sub'] == this_uri]
            ss = []
            for edge in this_edges:
                s = self.make_statement_from_edge(edge)
                if s and s.get_value():
                    ss.append(s)

            # set instance of using the root node
            root_nodes = self.root_node[node.id_uri]
            for root_node in root_nodes:
                # don't add instance of self!
                if root_node in self.uri_node_map and root_node != node.id_uri:
                    # print("{} root node {}".format(node.id_uri, root_node))
                    ref = node.create_ref_statement()
                    value_qid = self.uri_node_map[root_node].qid
                    if value_qid:
                        ss.append(wdi_core.WDItemID(value_qid, self.helper.get_pid('P31'), references=[ref]))

            if not ss:
                # there are no statements for this node
                continue

            # print("{}".format([(x.get_value(), x.get_prop_nr()) for x in ss]))
            item = wdi_core.WDItemEngine(
                wd_item_id=node.qid, data=ss,
                append_value=self.APPEND_PROPS,
                fast_run=self.FAST_RUN,
                fast_run_base_filter={node.id_pid: ''},
                fast_run_use_refs=True,
                global_ref_mode='CUSTOM',
                ref_handler=self.ref_handler,
                sparql_endpoint_url=self.sparql_endpoint_url,
                mediawiki_api_url=self.mediawiki_api_url,
                core_props=self.CORE_IDS
            )
            this_pid, this_value = cu.parse_curie(cu.uri_to_curie(this_uri))
            this_pid = self.helper.get_pid(this_pid)
            wdi_helpers.try_write(item, record_id=this_value, record_prop=this_pid,
                                  login=login, write=False)

    def make_statement_from_edge(self, edge):
        # we can override this to define a custom statement creator that makes a specific
        # statement depending on the edge or whatever else

        #  print("edge: {}".format(edge))
        # the predicate has to be defined explicitly
        pred_pid = self.PRED_PID_MAP[edge['pred']]
        self.pids.add(pred_pid)

        # The subject is the item that we have a node for
        subj_node = self.uri_node_map[edge['sub']]

        # the object is a URI either in this node or elsewhere
        obj_qid = self.get_object_qid(edge['obj'])

        if obj_qid:
            return wdi_core.WDItemID(obj_qid, pred_pid, references=[subj_node.create_ref_statement()])

    def get_object_qid(self, edge_obj):
        # object in an edge could be anything. it doesn't have to be a URI that exists within this graph
        # for example, we could be running the DO, and it have an object that is an UBERON class

        # first. check if this URI exists in our graph
        if edge_obj in self.uri_node_map:
            return self.uri_node_map[edge_obj].qid

        # if not, check if the prefix exists in wikidata
        try:
            obj_pid, obj_value = cu.parse_curie(cu.uri_to_curie(edge_obj))
        except Exception as e:
            m = wdi_helpers.format_msg(None, None, None, "edge object not found: {}".format(edge_obj))
            print(m)
            wdi_core.WDItemEngine.log("WARNING", m)
            return None

        obj_pid = self.helper.get_pid(obj_pid)
        # if this property exists, get all of the values for this property
        if obj_pid not in self.pid_id_mapper:
            print("loading: {}".format(obj_pid))
            id_map = wdi_helpers.id_mapper(obj_pid, return_as_set=True,
                                           prefer_exact_match=True,
                                           endpoint=self.sparql_endpoint_url)
            self.pid_id_mapper[obj_pid] = id_map if id_map else dict()

        # look up by the value
        if obj_value in self.pid_id_mapper[obj_pid]:
            obj_qids = self.pid_id_mapper[obj_pid][obj_value]
            if len(obj_qids) == 1:
                return list(obj_qids)[0]
            else:
                m = wdi_helpers.format_msg(None, None, None,
                                           "multiple qids ({}) found for: {}".format(obj_qids, edge_obj))
                print(m)
                wdi_core.WDItemEngine.log("WARNING", m)
        else:
            m = wdi_helpers.format_msg(None, None, None, "no qids found for: {}".format(edge_obj))
            print(m)
            wdi_core.WDItemEngine.log("WARNING", m)

    def check_for_existing_deprecated_nodes(self):
        # check in wikidata if there are items with the primary ID of deprecated nodes
        dep_uri_qid = dict()  # key is uri, value is QID
        dep_uri = [x.id_uri for x in self.deprecated_nodes]
        for uri in dep_uri:
            try:
                pid, value = cu.parse_curie(cu.uri_to_curie(uri))
            except Exception:
                continue
            pid = self.helper.get_pid(pid)

            if pid not in self.pid_id_mapper:
                print("loading: {}".format(pid))
                id_map = wdi_helpers.id_mapper(pid, endpoint=self.sparql_endpoint_url)
                self.pid_id_mapper[pid] = id_map if id_map else dict()

            if value in self.pid_id_mapper[pid]:
                dep_uri_qid[uri] = self.pid_id_mapper[pid][value]
        print("the following should be checked and deleted: {}".format(dep_uri_qid))
        # todo: log
        return dep_uri_qid

    def _get_old_releases(self):
        # get all editions and filter out the current (i.e. latest release)
        if not self.release:
            raise ValueError("create release item first")
        releases_d = self.release.get_all_releases()
        releases_d = releases_d if releases_d else dict()
        releases = set(releases_d.values())
        releases.discard(self.release_qid)
        return releases

    def _get_all_pids_used(self):
        # get all the props we used
        all_pids = set(chain(*[x.pids for x in self.nodes]))
        all_pids.update(self.pids)
        print(all_pids)
        return all_pids

    def _get_fastrun_container(self):
        all_pids = self._get_all_pids_used()
        # get a fastrun container
        frc = self.nodes[0].item.fast_run_container
        if not frc:
            print("fastrun container not found. not removing deprecated statements")
            return None
        frc.clear()

        # populate the frc using all PIDs we've touched
        for prop in all_pids:
            frc.write_required([wdi_core.WDString("fake value", prop)])

        return frc

    def remove_deprecated_statements(self, login):
        releases = self._get_old_releases()
        frc = self._get_fastrun_container()

        for node in self.nodes:
            node.remove_deprecated_statements(releases, frc, login)

    def calculate_root_nodes(self):
        # build a directed graph in networkx using all is_a edges
        # for each node, get the descendants with an out_degree of 0
        # which are the root nodes for that node
        node_uris = set([x.id_uri for x in self.nodes])

        G = nx.DiGraph()
        for node in node_uris:
            G.add_node(node)
        # only use the edges for nodes in self.nodes
        edges = [e for e in self.edges if (e['sub'] in node_uris) and (e['obj'] in node_uris)]
        # is_a edges
        edges = [e for e in edges if e['pred'] == 'is_a']
        G.add_edges_from([(e['sub'], e['obj']) for e in edges])

        root_nodes = set([node for node in G.nodes if G.out_degree(node) == 0])

        root = dict()
        for node in node_uris:
            root[node] = root_nodes & nx.descendants(G, node)

        self.root_node = root
        self.G = G

    def find_replaced_nodes(self, login):
        # in GO atleast, some deprecated nodes are denoted as replaced by some other node
        # find these, so that the statements can be removed and then merge them into the new node
        # "http://purl.obolibrary.org/obo/IAO_0100001" : "term replaced by"

        dep_uri_qid = self.check_for_existing_deprecated_nodes()
        to_merge = dict()  # from: to
        for node in self.deprecated_nodes:
            if node.id_uri in dep_uri_qid:
                node.qid = dep_uri_qid[node.id_uri]
                if 'http://purl.obolibrary.org/obo/IAO_0100001' in node.bpv:
                    to_node = node.bpv['http://purl.obolibrary.org/obo/IAO_0100001']
                    assert len(to_node) == 1
                    to_merge[node.id_uri] = list(to_node)[0]

        # first remove the old statements off this node, then merge it
        releases = self._get_old_releases()
        frc = self._get_fastrun_container()
        for node_uri in to_merge:
            node = self.uri_node_map[node_uri]
            node.remove_deprecated_statements(releases, frc, login)

            # need to remove description or you get errors....
            item = wdi_core.WDItemEngine(wd_item_id=node.qid)
            item.set_description("")
            item.write(login)

            wdi_core.WDItemEngine.merge_items(node.qid, self.uri_node_map[to_merge[node_uri]].qid, login)

    def print_prop_usage(self):
        from collections import Counter
        from pprint import pprint
        pprint(Counter([e['pred'] for e in self.edges]).most_common())

    def print_xref_usage(self):
        from collections import Counter
        from pprint import pprint
        from itertools import chain
        xrefs = set(chain(*[x.xrefs for x in self.nodes]))
        pprint(Counter(x.split(":")[0] for x in xrefs).most_common())

    def __str__(self):
        return "{} {} #nodes:{} #edges:{}".format(self.NAME, self.version, len(self.nodes),
                                                  len(self.edges))
