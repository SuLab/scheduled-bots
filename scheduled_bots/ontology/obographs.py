"""
This is a generic OWL -> Wikidata importer
Requires a obographs JSON file made from an owl file

"""

import traceback
from time import strftime, gmtime

from tqdm import tqdm
from wikicurie.wikicurie import CurieUtil, default_curie_map
import json
from datetime import datetime

from scheduled_bots import utils
from wikidataintegrator import wdi_core, wdi_property_store, wdi_helpers, wdi_login
from wikidataintegrator.ref_handlers import update_retrieved_if_new

uri_to_curie = lambda s: s.split("/")[-1].replace("_", ":")
curie_map = default_curie_map.copy()
cu = CurieUtil(curie_map)


class Node:
    def __init__(self, json_node, graph):
        self.id_uri = json_node['id']  # e.g. http://purl.obolibrary.org/obo/DOID_8718
        self.id_curie = uri_to_curie(self.id_uri)
        self.label = json_node.get("lbl")
        self.type = json_node.get("type")
        self.graph = graph
        self.qid = None
        self.xrefs = set()

        meta = json_node.get("meta", dict())

        if 'xrefs' in meta:
            self.xrefs = set([x['val'] for x in meta['xrefs']])

        namespaces = [x['val'] for x in meta['basicPropertyValues'] if
                      x['pred'] == 'http://www.geneontology.org/formats/oboInOwl#hasOBONamespace'] if \
            'basicPropertyValues' in meta else []
        if namespaces:
            self.namespace = namespaces[0]
        else:
            self.namespace = graph.default_namespace

        self.descr = meta.get('definition', dict()).get('val')
        self.deprecated = meta.get('deprecated', False)
        self.synonyms = set(x['val'] for x in meta.get('synonyms', list()))
        # filter out the label from the synonyms
        self.synonyms.discard(self.label)

        self.qid = None

    def create_ref_statement(self):
        assert self.graph.release_qid, "create the release first (on the graph class)"

        primary_ext_id_pid, primary_ext_id = cu.parse_curie(self.id_curie)

        stated_in = wdi_core.WDItemID(value=self.graph.release_qid, prop_nr='P248', is_reference=True)
        ref_extid = wdi_core.WDExternalID(value=primary_ext_id, prop_nr=primary_ext_id_pid, is_reference=True)
        ref_retrieved = wdi_core.WDTime(strftime("+%Y-%m-%dT00:00:00Z", gmtime()), prop_nr='P813', is_reference=True)
        reference = [stated_in, ref_retrieved, ref_extid]
        return reference

    def set_label(self, wd_item):
        # only setting the label if its currently blank or a new item is being created
        if wd_item.get_label() == "":
            wd_item.set_label(self.label)

    def set_descr(self, wd_item):
        # if the current description is blank and the new description
        # is something else (and not over 250 characters), use it
        current_descr = wd_item.get_description()
        if current_descr.lower() in {""} and self.descr and len(self.descr) < 250:
            wd_item.set_description(utils.clean_description(self.descr))
        elif current_descr.lower() == "":
            wd_item.set_description(self.graph.DEFAULT_DESCRIPTION)

    def set_aliases(self, wd_item):
        if self.synonyms is not None:
            wd_item.set_aliases(aliases=self.synonyms, append=True)

    def create(self, login, write=True):
        # create or get qid
        # creates the primary external ID, the xrefs, instance of (if set), checks label, description, and aliases
        # not other properties (i.e. subclass), as these may require items existing that may not exist yet

        ref = self.create_ref_statement()

        primary_ext_id_pid, primary_ext_id = cu.parse_curie(self.id_curie)

        # kind of hacky way to make sure this ID is unique
        assert primary_ext_id_pid in wdi_property_store.wd_properties
        wdi_property_store.wd_properties[primary_ext_id_pid]['core_id'] = True

        s = [wdi_core.WDExternalID(primary_ext_id, primary_ext_id_pid, references=[ref])]

        for xref in self.xrefs:
            if xref.split(":")[0] not in cu.curie_map:
                continue
            pid, ext_id = cu.parse_curie(xref)
            s.append(wdi_core.WDExternalID(ext_id, pid, references=[ref]))

        # set instance of using the namespace
        if self.namespace in self.graph.NAMESPACE_QID:
            s.append(wdi_core.WDItemID(self.graph.NAMESPACE_QID[self.namespace], 'P31', references=[ref]))

        try:

            wd_item = wdi_core.WDItemEngine(
                item_name=self.label, data=s, domain="this doesn't do anything",
                append_value=self.graph.APPEND_PROPS,
                fast_run=self.graph.FAST_RUN,
                fast_run_base_filter=self.graph.FAST_RUN_FILTER,
                fast_run_use_refs=True,
                global_ref_mode='CUSTOM',
                ref_handler=update_retrieved_if_new
            )
        except Exception as e:
            # traceback.print_exc()
            msg = wdi_helpers.format_msg(primary_ext_id, primary_ext_id_pid, None, str(e), msg_type=type(e))
            wdi_core.WDItemEngine.log("ERROR", msg)
            return

        self.set_label(wd_item)
        self.set_descr(wd_item)
        self.set_aliases(wd_item)

        wdi_helpers.try_write(wd_item, record_id=primary_ext_id, record_prop=primary_ext_id_pid,
                              login=login, write=write)

        self.qid = wd_item.wd_item_id


class Graph:
    # the following MUST be overridden in a subclass !!!
    NAME = None
    QID = None
    DEFAULT_DESCRIPTION = None
    APPEND_PROPS = None
    FAST_RUN = None
    FAST_RUN_FILTER = None
    PRED_PID_MAP = None
    NAMESPACE_QID = None

    # the following must be overriden if a custom node type is being used
    NODE_CLASS = Node

    def __init__(self):
        assert self.NAME, "Must initialize subclass"
        self.version = None
        self.date = None
        self.default_namespace = None
        self.json_graph = None
        self.nodes = None
        self.edges = None

        self.release_qid = None
        self.pid_id_mapper = dict()
        self.uri_node_map = dict()

    def parse_graph(self, json_path, graph_uri):
        with open(json_path) as f:
            d = json.load(f)
        graphs = {g['id']: g for g in d['graphs']}

        self.json_graph = graphs[graph_uri]
        self.parse_meta()
        self.parse_nodes()
        self.filter_nodes()
        self.parse_edges()
        self.filter_edges()

    def parse_meta(self):
        meta = self.json_graph['meta']

        # this is a PURI to the release owl file
        # e.g. : http://purl.obolibrary.org/obo/doid/releases/2018-03-02/doid.owl
        self.version = meta['version']
        # convert the version string to a version string to use for the release item in WD
        self.edition = self.version.rsplit("/", 2)[1]
        # date is for the release item as well
        self.date = datetime.strptime(self.edition, '%Y-%m-%d')

        self.default_namespace = [x['val'] for x in meta['basicPropertyValues'] if
                                  x['pred'] == 'http://www.geneontology.org/formats/oboInOwl#default-namespace'][0]

    def parse_nodes(self):
        self.nodes = [self.NODE_CLASS(json_node, self) for json_node in self.json_graph['nodes']]
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
        self.nodes = [x for x in self.nodes if not x.deprecated and x.type == "CLASS"]

    def create_nodes(self, login, write=True):
        for node in tqdm(self.nodes, desc="creating items"):
            node.create(login, write=write)

    def create_release(self, login):
        r = wdi_helpers.Release('{} release {}'.format(self.NAME, self.edition),
                                'Release of the {}'.format(self.NAME),
                                self.edition,
                                archive_url=self.version, edition_of_wdid=self.QID,
                                pub_date=self.date.date().strftime('+%Y-%m-%dT%H:%M:%SZ'))
        wd_item_id = r.get_or_create(login)
        if wd_item_id:
            self.release_qid = wd_item_id
        else:
            raise ValueError("unable to create release")

    def create_edges(self, login, write=True):
        # get all the edges for a single subject
        # TODO: this skips edges where the subject is not one of our nodes
        for node in tqdm(self.nodes, desc="creating edges"):
            this_uri = node.id_uri
            this_edges = [edge for edge in self.edges if edge['sub'] == this_uri]
            ss = []
            for edge in this_edges:
                s = self.make_statement_from_edge(edge)
                if s:
                    ss.append(s)
            if ss:
                item = wdi_core.WDItemEngine(
                    wd_item_id=node.qid, data=ss, domain="fake news",
                    append_value=self.APPEND_PROPS,
                    fast_run=self.FAST_RUN,
                    fast_run_base_filter=self.FAST_RUN_FILTER,
                    fast_run_use_refs=True,
                    global_ref_mode='CUSTOM',
                    ref_handler=update_retrieved_if_new
                )
                this_pid, this_value = cu.parse_curie(uri_to_curie(this_uri))
                wdi_helpers.try_write(item, record_id=this_value, record_prop=this_pid,
                                      login=login, write=write)

    def make_statement_from_edge(self, edge):
        print(edge)

        # the predicate has to be defined explicitly
        pred_pid = self.PRED_PID_MAP[edge['pred']]

        # The subject is the item that we have a node for
        subj_node = self.uri_node_map[edge['sub']]

        # for the obj the value can be anything, we need to figure out the pid for the uri type and qid of that item
        # but first, check if its one of our nodes
        obj_qid = None
        if edge['obj'] in self.uri_node_map:
            obj_qid = self.uri_node_map[edge['obj']].qid
        else:
            # its to an external ontology, find it in wikidata
            obj_pid, obj_value = cu.parse_curie(uri_to_curie(edge['obj']))
            if obj_pid not in self.pid_id_mapper:
                self.pid_id_mapper[obj_pid] = wdi_helpers.id_mapper(obj_pid, return_as_set=True,
                                                                    prefer_exact_match=True)
            if obj_value in self.pid_id_mapper[obj_pid]:
                obj_qids = self.pid_id_mapper[obj_pid][obj_value]
                if len(obj_qids) == 1:
                    obj_qid = list(obj_qids)[0]
                else:
                    print("oh no: {} {}".format(obj_pid, obj_value))
            else:
                print("oh no: {} {}".format(obj_pid, obj_value))

        print(subj_node.qid, pred_pid, obj_qid)
        if obj_qid:
            return wdi_core.WDItemID(obj_qid, pred_pid, references=[subj_node.create_ref_statement()])

    def __str__(self):
        return "{} {} #nodes:{} #edges:{}".format(self.default_namespace, self.version, len(self.nodes),
                                                  len(self.edges))
