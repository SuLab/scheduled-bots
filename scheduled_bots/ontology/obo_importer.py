"""
A OBO bot for EBI ontologies
"""
import json
import os
import pprint
import sys
import time
import traceback
from datetime import datetime

import argparse
import requests
from wikidataintegrator import wdi_core, wdi_login, wdi_helpers

__author__ = 'Sebastian Burgstaller'
__license__ = 'MIT'
__metadata__ = {'name': 'GoOntologyBot',
                'tags': ['GO'],
                }

try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

ONTOLOGIES = {'GO': {'ontology_ref_item': 'Q135085',  # 'Gene Ontolgy' item
                     'core_property_nr': 'P686',
                     # biological process (GO:0008150), molecular function (GO:0003674), cellular component (GO:0005575) (Q5058355)
                     'root_objects': ['0008150', '0003674', '0005575'],
                     'use_prefix': True}}


def ec_formatter(ec_number):
    splits = ec_number.split('.')
    if len(splits) < 4:
        for x in range(4 - len(splits)):
            splits.append('-')

    return '.'.join(splits)


class OBOImporter(object):
    obo_wd_map = {
        'http://www.w3.org/2000/01/rdf-schema#subClassOf': {'P279': ''},  # subclassOf aka 'is a'
        'http://purl.obolibrary.org/obo/BFO_0000051': {'P527': ''},  # has_part
        'http://purl.obolibrary.org/obo/BFO_0000050': {'P361': ''},  # part of

        'http://purl.obolibrary.org/obo/RO_0002211': {'P128': ''},  # regulates
        'http://purl.obolibrary.org/obo/RO_0002212': {'P128': {'P794': 'Q22260640'}},
    # negatively regulates WD item: Q22260640
        'http://purl.obolibrary.org/obo/RO_0002213': {'P128': {'P794': 'Q22260639'}},
    # positively regulates WD item: Q22260639
    }

    rev_prop_map = {
        'http://purl.obolibrary.org/obo/BFO_0000051': 'http://purl.obolibrary.org/obo/BFO_0000050',
        'http://purl.obolibrary.org/obo/BFO_0000050': 'http://purl.obolibrary.org/obo/BFO_0000051'
    }

    xref_props = {
        'UBERON': 'P1554',
        'MSH': 'P486',
        'NCI': 'P1748',  # NCI thesaurus, there exists a second NCI property
        'CHEBI': 'P683',
        'OMIM': 'P492',
        'EC': 'P591',
    }

    xref_val_mod_funcs = {
        'P591': ec_formatter,

    }

    base_onto_struct_props = [
        'http://www.w3.org/2000/01/rdf-schema#subClassOf',
    ]

    obo_synonyms = {}

    ols_session = requests.Session()

    def __init__(self, root_objects, ontology, core_property_nr, ontology_ref_item, login, local_qid_onto_map,
                 use_prefix=True, fast_run=True, fast_run_base_filter=None, write=True, sub_ontology=None):

        # run go prefix fixer before any attempts to make new go terms!
        self.login_obj = login
        self.root_objects = root_objects
        self.core_property_nr = core_property_nr
        self.ontology = ontology
        self.sub_ontology = sub_ontology
        self.ontology_ref_item = ontology_ref_item
        self.base_url = 'http://www.ebi.ac.uk/ols/api/ontologies/{}/terms/'.format(ontology)
        self.base_url += 'http%253A%252F%252Fpurl.obolibrary.org%252Fobo%252F'
        self.use_prefix = use_prefix
        self.fast_run = fast_run
        self.fast_run_base_filter = fast_run_base_filter
        self.write = write

        self.headers = {
            'Accept': 'application/json'
        }

        # as SPARQL endpoint updates are not fast enough, a local temporary mapping is required
        self.local_qid_onto_map = local_qid_onto_map
        # pprint.pprint(self.local_qid_onto_map)

        for ro in root_objects:
            if not self.sub_ontology:
                so = self.get_sub_ontology(ro)
            else:
                so = self.sub_ontology
          
            if ro in self.local_qid_onto_map and self.local_qid_onto_map[ro]['had_root_write'] and \
                    ('children' in self.local_qid_onto_map[ro] or 'parents' in self.local_qid_onto_map[ro]):

                OBOImporter(root_objects=self.local_qid_onto_map[ro]['children'], ontology=ontology,
                            core_property_nr=self.core_property_nr, ontology_ref_item=self.ontology_ref_item,
                            login=login, local_qid_onto_map=self.local_qid_onto_map, use_prefix=self.use_prefix,
                            fast_run=self.fast_run, fast_run_base_filter=self.fast_run_base_filter, write=self.write,
                            sub_ontology=so)
            else:
                try:
                    r = OBOImporter.ols_session.get(url=self.base_url + '{}_{}/graph'.format(self.ontology, ro),
                                                    headers=self.headers)
                    self.term_graph = r.json()
                except requests.HTTPError as e:
                    print(e)
                    continue
                except ValueError as e:
                    print(e)
                    continue

                print(r.url)
                pprint.pprint(self.term_graph)

                children = []
                parents = []
                for edge in self.term_graph['edges']:
                    if edge['label'] == 'is a':
                        # only accept terms from a certain ontology, ignore other (OWL) targets.
                        if len(edge['source'].split('_')) > 1:
                            graph_source = edge['source'].split('_')[1]
                        else:
                            continue

                        if graph_source == ro:
                            if len(edge['target'].split('_')) > 1:
                                parents.append(edge['target'].split('_')[1])
                            else:
                                continue
                        else:
                            children.append(graph_source)

                self.write_term(current_root_id=ro, parents=set(parents), children=set(children))

                OBOImporter(root_objects=children, ontology=ontology, core_property_nr=self.core_property_nr,
                            ontology_ref_item=self.ontology_ref_item, login=login,
                            local_qid_onto_map=self.local_qid_onto_map, use_prefix=self.use_prefix,
                            fast_run=self.fast_run, fast_run_base_filter=self.fast_run_base_filter, write=self.write, 
                            sub_ontology=so)

    def write_term(self, current_root_id, parents, children):
        print('current_root', current_root_id, parents, children)
        current_node_qids = []

        def get_item_qid(go_id, data=()):
            if self.use_prefix:
                id_string = '{}:{}'.format(self.ontology, go_id)
            else:
                id_string = go_id

            # for efficiency reasons, skip if item already had a root write performed
            if go_id in self.local_qid_onto_map and self.local_qid_onto_map[go_id]['had_root_write'] \
                    and 'qid' in self.local_qid_onto_map[go_id]:
                return self.local_qid_onto_map[go_id]['qid'], False, False

            try:
                data = list(data)

                r = OBOImporter.ols_session.get(url=self.base_url + '{}_{}'.format(self.ontology, go_id),
                                                headers=self.headers)
                go_term_data = r.json()
                label = go_term_data['label'].replace('_', ' ')

                description = go_term_data['description'][0]

                if go_term_data['is_obsolete']:
                    OBOImporter.cleanup_obsolete_edges(ontology_id=id_string,
                                                       login=self.login_obj, core_property_nr=self.core_property_nr,
                                                       obsolete_term=True, write=write)
                    return None, None, None

                # get parent ontology term info so item can be populated with description, etc.
                data.append(wdi_core.WDString(value=id_string, prop_nr=self.core_property_nr,
                                              references=[self.create_reference()]))

                exact_match_string = 'http://purl.obolibrary.org/obo/{}_{}'.format(self.ontology, go_id)
                data.append(wdi_core.WDUrl(value=exact_match_string, prop_nr='P2888'))
                
                # add instance of sub-ontology
                if self.sub_ontology:
                    data.append(wdi_core.WDItemID(value=self.sub_ontology, prop_nr='P31',
                                references=[self.create_reference()]))

                # add xrefs
                if go_term_data['obo_xref']:
                    for xref in go_term_data['obo_xref']:
                        if xref['database'] in OBOImporter.xref_props:
                            wd_prop = OBOImporter.xref_props[xref['database']]
                        else:
                            continue
                        if wd_prop in OBOImporter.xref_val_mod_funcs:
                            xref_value = OBOImporter.xref_val_mod_funcs[wd_prop](xref['id'])
                        else:
                            xref_value = xref['id']
                        data.append(wdi_core.WDExternalID(value=xref_value, prop_nr=wd_prop,
                                                          references=[self.create_reference()]))

                if go_term_data['obo_synonym']:
                    for syn in go_term_data['obo_synonym']:
                        if syn['type'] in OBOImporter.obo_synonyms:
                            wd_prop = OBOImporter.obo_synonyms[syn['type']]
                        else:
                            continue
                        syn_value = syn['name']
                        data.append(wdi_core.WDExternalID(value=syn_value, prop_nr=wd_prop,
                                                          references=[self.create_reference()]))

                if go_id in self.local_qid_onto_map:
                    wd_item = wdi_core.WDItemEngine(wd_item_id=self.local_qid_onto_map[go_id]['qid'], domain='obo',
                                                    data=data, fast_run=self.fast_run,
                                                    fast_run_base_filter=self.fast_run_base_filter,
                                                    global_ref_mode='STRICT_OVERWRITE')
                else:
                    wd_item = wdi_core.WDItemEngine(item_name='test', domain='obo', data=data, fast_run=self.fast_run,
                                                    fast_run_base_filter=self.fast_run_base_filter,
                                                    global_ref_mode='STRICT_OVERWRITE')
                if wd_item.get_label() == "":
                    wd_item.set_label(label=label)
                if wd_item.get_description() == "":
                    wd_item.set_description(description=description[:250])
                # if len(description) <= 250:
                #     wd_item.set_description(description=description)
                # else:
                #     wd_item.set_description(description='Gene Ontology term')
                if go_term_data['synonyms'] is not None and len(go_term_data['synonyms']) > 0:
                    aliases = []
                    for alias in go_term_data['synonyms']:
                        if len(alias) <= 250:
                            aliases.append(alias)

                    wd_item.set_aliases(aliases=aliases)

                success = wdi_helpers.try_write(wd_item, go_id, self.core_property_nr, self.login_obj, write=self.write)
                qid = wd_item.wd_item_id

                if go_id not in self.local_qid_onto_map:
                    self.local_qid_onto_map[go_id] = {
                        'qid': qid,
                        'had_root_write': False,
                    }

                if go_id == current_root_id:
                    self.local_qid_onto_map[go_id]['had_root_write'] = True
                    self.local_qid_onto_map[go_id]['parents'] = list(parents)
                    self.local_qid_onto_map[go_id]['children'] = list(children)

                current_node_qids.append(qid)
                print('QID created or retrieved', qid)
                if success is True:
                    return qid, go_term_data['obo_xref'], wd_item.require_write
                else:
                    return None, None, None
            except Exception as e:
                exc_info = sys.exc_info()
                traceback.print_exception(*exc_info)
                msg = wdi_helpers.format_msg(go_id, self.core_property_nr, None, str(e), msg_type=type(e))
                wdi_core.WDItemEngine.log("ERROR", msg)
                return None, None, None

        dt = []
        parent_qids = []
        write_reqired = []
        for parent_id in parents:
            pi, o, w = get_item_qid(parent_id)
            write_reqired.append(w)

            if pi:
                parent_qids.append(pi)
                dt.append(wdi_core.WDItemID(value=pi, prop_nr='P279', references=[self.create_reference()]))

        for edge in self.term_graph['edges']:
            if edge['uri'] in self.obo_wd_map and edge['uri'] != 'http://www.w3.org/2000/01/rdf-schema#subClassOf':
                go = edge['target'].split('_')[-1]
                if go != current_root_id:
                    xref_dict = self.obo_wd_map[edge['uri']]
                elif edge['uri'] in self.rev_prop_map and edge['source'].split('_')[-1] != current_root_id:
                    xref_dict = self.obo_wd_map[self.rev_prop_map[edge['uri']]]
                    go = edge['source'].split('_')[-1]
                else:
                    continue

                pi, o, w = get_item_qid(go_id=go)
                write_reqired.append(w)
                dt.append(self.create_xref_statement(value=pi, xref_dict=xref_dict))

        root_qid, obsolete, w = get_item_qid(go_id=current_root_id, data=dt)
        if obsolete and not any(write_reqired):
            if self.use_prefix:
                id_string = '{}:{}'.format(self.ontology, current_root_id)
            else:
                id_string = current_root_id

            OBOImporter.cleanup_obsolete_edges(ontology_id=id_string,
                                               login=self.login_obj, core_property_nr=self.core_property_nr,
                                               current_node_qids=current_node_qids, write=self.write)

        print('----COUNT----:', len(self.local_qid_onto_map))
        f = open('temp_{}_onto_map.json'.format(self.ontology), 'w')
        f.write(json.dumps(self.local_qid_onto_map))
        f.close()

    def create_reference(self):
        refs = [
            wdi_core.WDItemID(value=self.ontology_ref_item, prop_nr='P248', is_reference=True),
            wdi_core.WDItemID(value='Q22230760', prop_nr='P143', is_reference=True),
            wdi_core.WDTime(time=time.strftime('+%Y-%m-%dT00:00:00Z', time.gmtime()), prop_nr='P813',
                            is_reference=True),
            # wdi_core.WDItemID(value='Q1860', prop_nr='P407', is_reference=True),  # language of work
        ]
        # old references should be overwritten
        # refs[0].overwrite_references = True

        return refs

    def create_xref_statement(self, value, xref_dict):
        for prop_nr, v in xref_dict.items():
            qualifiers = []
            if v:
                for p, vv in v.items():
                    qualifiers.append(wdi_core.WDItemID(value=vv, prop_nr=p, is_qualifier=True))

            return wdi_core.WDItemID(value=value, prop_nr=prop_nr, qualifiers=qualifiers,
                                     references=[self.create_reference()])
          
    def get_sub_ontology(self, oid):
        ontology_id = oid if oid.startswith(self.ontology) else '{}:{}'.format(self.ontology, oid)
        query = '''
        SELECT DISTINCT * WHERE {{
            ?qid wdt:{} '{}' .
        }}
        '''.format(self.core_property_nr, ontology_id)

        r = wdi_core.WDItemEngine.execute_sparql_query(query=query)

        for x in r['results']['bindings']:
            return x['qid']['value'].split('/').pop()

    @staticmethod
    def cleanup_obsolete_edges(ontology_id, core_property_nr, login, current_node_qids=(), obsolete_term=False, write=True):
        filter_props_string = ''
        if not obsolete_term:
            for x in OBOImporter.obo_wd_map.values():
                prop_nr = list(x.keys())[0]
                filter_props_string += 'Filter (?p = wdt:{})\n'.format(prop_nr)

        query = '''
        SELECT DISTINCT ?qid ?p ?onto_qid WHERE {{
            {{
                SELECT DISTINCT ?onto_qid WHERE {{
                    ?onto_qid wdt:{2} '{0}' .
                }}
            }}
            ?qid ?p [wdt:{2} '{0}'].
            {1}
        }}
        ORDER BY ?qid
        '''.format(ontology_id, filter_props_string, core_property_nr)
        print(query)

        sr = wdi_core.WDItemEngine.execute_sparql_query(query=query)

        for occurrence in sr['results']['bindings']:
            if 'statement' in occurrence['qid']['value']:
                continue

            qid = occurrence['qid']['value'].split('/')[-1]
            if qid in current_node_qids:
                continue

            prop_nr = occurrence['p']['value'].split('/')[-1]
            wd_onto_qid = occurrence['onto_qid']['value'].split('/')[-1]
            wd_item_id = wdi_core.WDItemID(value=wd_onto_qid, prop_nr=prop_nr)
            setattr(wd_item_id, 'remove', '')
            try:
                wd_item = wdi_core.WDItemEngine(wd_item_id=qid, data=[wd_item_id])
                wdi_helpers.try_write(wd_item, ontology_id, core_property_nr, login,
                                      edit_summary="removed obsolete edges", write=write)
            except Exception as e:
                exc_info = sys.exc_info()
                traceback.print_exception(*exc_info)
                msg = wdi_helpers.format_msg(ontology_id, core_property_nr, None, str(e), msg_type=type(e))
                wdi_core.WDItemEngine.log("ERROR", msg)

        if obsolete_term:
            data = [
                wdi_core.WDString(value=ontology_id, prop_nr=core_property_nr, rank='deprecated'),
            ]

            try:
                wd_item = wdi_core.WDItemEngine(item_name='obo', domain='obo', data=data, use_sparql=True)
                if wd_item.create_new_item:
                    return

                wdi_helpers.try_write(wd_item, ontology_id, core_property_nr, login,
                                      edit_summary="obsoleted", write=write)
            except Exception as e:
                exc_info = sys.exc_info()
                traceback.print_exception(*exc_info)
                msg = wdi_helpers.format_msg(ontology_id, core_property_nr, None, str(e), msg_type=type(e))
                wdi_core.WDItemEngine.log("ERROR", msg)


def run(ontology, login, write=True):
    ontology_ref_item = ONTOLOGIES[ontology]['ontology_ref_item']
    core_property_nr = ONTOLOGIES[ontology]['core_property_nr']
    root_objects = ONTOLOGIES[ontology]['root_objects']
    use_prefix = ONTOLOGIES[ontology]['use_prefix']

    file_name = 'temp_{}_onto_map.json'.format(ontology)
    if os.path.exists(file_name):
        f = open(file_name, 'r')
        local_qid_onto_map = json.loads(f.read())
        f.close()
    else:
        local_qid_onto_map = {}

    OBOImporter(root_objects=root_objects, ontology=ontology, core_property_nr=core_property_nr,
                ontology_ref_item=ontology_ref_item, login=login, local_qid_onto_map=local_qid_onto_map,
                use_prefix=use_prefix, fast_run=True, fast_run_base_filter={core_property_nr: ''}, write=write)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='run EBI ontology bot')
    parser.add_argument('ontology', help='ontology to run', type=str)
    parser.add_argument('--log-dir', help='directory to store logs', type=str)
    parser.add_argument('--dummy', help='do not actually do write', action='store_true')
    parser.add_argument('--fastrun', dest='fastrun', action='store_true')
    parser.add_argument('--no-fastrun', dest='fastrun', action='store_false')
    parser.set_defaults(fastrun=True)
    args = parser.parse_args()
    ontology = args.ontology
    log_dir = args.log_dir if args.log_dir else "./logs"
    fast_run = args.fastrun
    run_id = datetime.now().strftime('%Y%m%d_%H:%M')
    __metadata__['run_id'] = run_id
    log_name = '{}-{}.log'.format(__metadata__['name'], run_id)

    if ontology not in ONTOLOGIES:
        raise ValueError("ontology {} not found".format(ontology))

    wdi_core.WDItemEngine.setup_logging(logger_name='WD_logger', log_name=log_name,
                                        header=json.dumps(__metadata__))

    login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS)

    run(ontology, login, write=not args.dummy)
