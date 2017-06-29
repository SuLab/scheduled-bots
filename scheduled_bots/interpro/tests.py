import copy
from wikidataintegrator import wdi_core, wdi_login, wdi_helpers
from scheduled_bots.interpro.IPRTerm import IPRTerm
from scheduled_bots.interpro.ProteinBot import create_for_one_protein


login = wdi_login.WDLogin("Gstupp", "sulab.org")

def __test_one_iprterm():
    doc = {
        "_id": "IPR000001",
        "protein_count": 3728,
        "children": None,
        "name": "Kringle",
        "parent": "IPR013806",
        "id": "IPR000001",
        "short_name": "Kringle",
        "found_in": [
            "IPR001314",
            "IPR003966",
            "IPR014394",
            "IPR016247",
            "IPR017076"
        ],
        "contains": [
            "IPR018056"
        ],
        "type": "Domain"
    }
    doc["release_wdid"] = "Q29947749"  # InterPro Release 63.0

    term = IPRTerm(**doc)
    item = term.create_item(login)
    term.create_relationships(login)

def __test_one_protein():
    doc = {
        "_id" : "Q03135",
        "has_part" : [
            "IPR018361"
        ],
        "subclass" : [
            "IPR001612"
        ]
    }
    UNIPROT = "P352"
    taxon = "Q15978631"
    uniprot2wd = wdi_helpers.id_mapper(UNIPROT, (("P703", taxon),))
    fast_run_base_filter = {UNIPROT: "", "P703": taxon}
    item = create_for_one_protein(login, doc, "Q29947749", uniprot2wd, fast_run_base_filter)


def __test_delete_statements_protein():
    UNIPROT = "P352"
    taxon = "Q15978631"
    uniprot2wd = wdi_helpers.id_mapper(UNIPROT, (("P703", taxon),))
    fast_run_base_filter = {UNIPROT: "", "P703": taxon}
    # in fastrun mode
    itemfr = wdi_core.WDItemEngine(wd_item_id="Q21115385", fast_run=True, fast_run_base_filter=fast_run_base_filter,
                                   data = [wdi_core.WDString("Q03135", "P352")])
    itemfr.fast_run_container.reconstruct_statements(itemfr.wd_item_id)

    # in non fastrun mode
    item = wdi_core.WDItemEngine(wd_item_id="Q21115385")



def test_check_one_json_representation():
    # give bot one doc, check the json representation

    expected_result = ({'type': 'statement', 'rank': 'normal', 'qualifiers-order': [],
                        'mainsnak': {'property': 'P2926', 'snaktype': 'value',
                                     'datavalue': {'type': 'string', 'value': 'IPR000001'}, 'datatype': 'external-id'},
                        'references': [{'snaks': {'P2926': [{'property': 'P2926', 'snaktype': 'value',
                                                             'datavalue': {'type': 'string', 'value': 'IPR000001'},
                                                             'datatype': 'string'}], 'P248': [
                            {'property': 'P248', 'snaktype': 'value', 'datatype': 'wikibase-item'}]},
                                        'snaks-order': ['P248', 'P2926']}], 'qualifiers': {}},
                       {'type': 'statement', 'rank': 'normal', 'qualifiers-order': [],
                        'mainsnak': {'property': 'P31', 'snaktype': 'value', 'datavalue': {'type': 'wikibase-entityid',
                                                                                           'value': {'id': 'Q898273',
                                                                                                     'entity-type': 'item',
                                                                                                     'numeric-id': 898273}},
                                     'datatype': 'wikibase-item'}, 'references': [{'snaks': {'P2926': [
                           {'property': 'P2926', 'snaktype': 'value',
                            'datavalue': {'type': 'string', 'value': 'IPR000001'}, 'datatype': 'string'}], 'P248': [
                           {'property': 'P248', 'snaktype': 'value', 'datatype': 'wikibase-item'}]},
                           'snaks-order': ['P248', 'P2926']}],
                        'qualifiers': {}})

    term = IPRTerm(**doc)
    wditem = term.create_item()
    actual_result = (d.get_json_representation() for d in wditem.data)
    assert expected_result == actual_result


def test_one_dont_create():
    term = IPRTerm(**doc)
    wditem = term.create_item()
    assert wditem.create_new_item is False


def test_one_require_write():
    term = IPRTerm(**write_doc)
    wditem = term.create_item()
    assert wditem.create_new_item is False
    assert wditem.require_write is True


def sandbox_edit():
    sandbox_qid = "Q4115189"
    term = IPRTerm(**doc)
    wditem = term.create_item()
    wditem.wd_item_id = sandbox_qid
