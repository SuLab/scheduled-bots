import copy

from .IPRTerm import IPRTerm

# a typical document
doc = {
    "_id": "IPR000001",
    "parent": "IPR013806",
    "protein_count": 3451,
    "name": "Kringle",
    "contains": ["IPR018056"],
    "type": "Domain",
    "id": "IPR000001",
    "found_in": ["IPR001314", "IPR003966", "IPR014394", "IPR016247", "IPR017076"],
    "short_name": "Kringle",
    "children": None
}

# a document that will require a write
write_doc = copy.copy(doc)
write_doc['name'] = "greg is great"


def test_check_one_json_representation():
    # give bot one doc, check the json representation

    expected_result = ({'type': 'statement', 'rank': 'normal', 'qualifiers-order': [], 'mainsnak': {'property': 'P2926', 'snaktype': 'value', 'datavalue': {'type': 'string', 'value': 'IPR000001'}, 'datatype': 'external-id'}, 'references': [{'snaks': {'P2926': [{'property': 'P2926', 'snaktype': 'value', 'datavalue': {'type': 'string', 'value': 'IPR000001'}, 'datatype': 'string'}], 'P248': [{'property': 'P248', 'snaktype': 'value', 'datatype': 'wikibase-item'}]}, 'snaks-order': ['P248', 'P2926']}], 'qualifiers': {}}, {'type': 'statement', 'rank': 'normal', 'qualifiers-order': [], 'mainsnak': {'property': 'P31', 'snaktype': 'value', 'datavalue': {'type': 'wikibase-entityid', 'value': {'id': 'Q898273', 'entity-type': 'item', 'numeric-id': 898273}}, 'datatype': 'wikibase-item'}, 'references': [{'snaks': {'P2926': [{'property': 'P2926', 'snaktype': 'value', 'datavalue': {'type': 'string', 'value': 'IPR000001'}, 'datatype': 'string'}], 'P248': [{'property': 'P248', 'snaktype': 'value', 'datatype': 'wikibase-item'}]}, 'snaks-order': ['P248', 'P2926']}], 'qualifiers': {}})

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
