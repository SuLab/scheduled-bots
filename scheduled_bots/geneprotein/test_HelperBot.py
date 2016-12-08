from .HelperBot import make_ref_source, check_record


def test_check_record():
    record = {'entrezgene': 123, 'genomic_pos': {}, 'type_of_gene':1, 'name':'g', 'ensembl': 2}
    try:
        check_record(record)
    except AssertionError:
        pass


def test_make_reference_timestamp():
    ref = make_ref_source({'id': 'entrez', 'timestamp': '20161204'}, 'entrez_gene', '1234')
    correct_json_rep = [{'P248': [{'datatype': 'wikibase-item',
                                   'datavalue': {'type': 'wikibase-entityid',
                                                 'value': {'entity-type': 'item',
                                                           'id': 'Q20641742',
                                                           'numeric-id': 20641742}},
                                   'property': 'P248',
                                   'snaktype': 'value'}]},
                        {'P813': [{'datatype': 'time',
                                   'datavalue': {'type': 'time',
                                                 'value': {'after': 0,
                                                           'before': 0,
                                                           'calendarmodel': 'http://www.wikidata.org/entity/Q1985727',
                                                           'precision': 11,
                                                           'time': '+2016-12-04T00:00:00Z',
                                                           'timezone': 0}},
                                   'property': 'P813',
                                   'snaktype': 'value'}]},
                        {'P351': [{'datatype': 'string',
                                   'datavalue': {'type': 'string', 'value': '1234'},
                                   'property': 'P351',
                                   'snaktype': 'value'}]}]
    assert [x.get_json_representation() for x in ref] == correct_json_rep


def test_make_reference_release():
    ref = make_ref_source({'id': 'ensembl', 'release': '86'}, 'ensembl_gene', '1234')
    correct_json_rep = [{'P248': [{'datatype': 'wikibase-item',
                                   'datavalue': {'type': 'wikibase-entityid',
                                                 'value': {'entity-type': 'item',
                                                           'id': 'Q27613766',
                                                           'numeric-id': 27613766}},
                                   'property': 'P248',
                                   'snaktype': 'value'}]},
                        {'P594': [{'datatype': 'string',
                                   'datavalue': {'type': 'string', 'value': '1234'},
                                   'property': 'P594',
                                   'snaktype': 'value'}]}]

    assert [x.get_json_representation() for x in ref] == correct_json_rep
