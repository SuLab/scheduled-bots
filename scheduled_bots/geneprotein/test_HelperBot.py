from .HelperBot import make_ref_source, validate_doc_eukaryotic, validate_doc_microbial
from nose.tools import assert_raises

# PASS
human_gene_minimum = {'_id': '105377262',
                      'entrezgene': 105377262,
                      'name': 'uncharacterized LOC105377262',
                      'refseq': {'genomic': ['NC_000004.12', 'NC_018915.2'],
                                 'rna': ['XR_938847.2', 'XR_938848.2', 'XR_949449.2', 'XR_949450.2']},
                      'symbol': 'LOC105377262',
                      'taxid': 9606,
                      'type_of_gene': 'ncRNA'}

# PASS
human_gene_pc = {'_id': '102724770',
                 'ensembl': {'gene': 'ENSG00000278817',
                             'protein': 'ENSP00000482514',
                             'transcript': 'ENST00000613204',
                             'translation': [{'protein': 'ENSP00000482514', 'rna': 'ENST00000613204'}]},
                 'entrezgene': 102724770,
                 'genomic_pos': {'chr': 'KI270734.1',
                                 'end': 137392,
                                 'start': 131494,
                                 'strand': 1},
                 'homologene': {'genes': [[9606, 8214],
                                          [9606, 102724770],
                                          [7955, 403055],
                                          [8364, 100125791],
                                          [9031, 395907],
                                          [9615, 608009],
                                          [9913, 100126445]],
                                'id': 136000},
                 'name': 'protein DGCR6',
                 'refseq': {'genomic': 'NT_187389.1',
                            'protein': 'XP_006724997.1',
                            'rna': ['XM_006724934.1', 'XR_951396.1'],
                            'translation': {'protein': 'XP_006724997.1', 'rna': 'XM_006724934.1'}},
                 'symbol': 'LOC102724770',
                 'taxid': 9606,
                 'type_of_gene': 'protein-coding',
                 'uniprot': {'Swiss-Prot': 'Q14129',
                             'TrEMBL': ['K7ELY4', 'K7EPQ2', 'Q6FGH4', 'X5D7D2']}}

# PASS
microbe_doc1 = {
    "_id": "884205",
    "_score": 13.733018,
    "entrezgene": 884205,
    "genomic_pos": {
        "chr": "NC_000117.1",
        "end": 937966,
        "start": 937409,
        "strand": 1
    },
    "locus_tag": "CT_799",
    "name": "50S ribosomal protein L25/general stress protein Ctc",
    "pir": "E71469",
    "refseq": {
        "genomic": "NC_000117.1",
        "protein": "NP_220319.1"
    },
    "symbol": "CT_799",
    "taxid": 272561,
    "type_of_gene": "protein-coding",
    "uniprot": {
        "Swiss-Prot": "O84805"
    }
}

# FAIL
fail_doc1 = {'_id': '105377262',
             'entrezgene': 105377262,
             'name': 'uncharacterized LOC105377262',
             'refseq': {'genomic': ['NC_000004.12', 'NC_018915.2'],
                        'rna': ['XR_938847.2', 'XR_938848.2', 'XR_949449.2', 'XR_949450.2']},
             'symbol': 'LOC105377262',
             'genomic_pos': [{'chr': 'KI270734.1',
                              'end': 137392,
                              'start': 131494,
                              'strand': 1},
                             {'chr': 'KI270734.1',
                              'end': 137392,
                              'start': 131494,
                              'strand': 1}],
             'taxid': 9606,
             'type_of_gene': 'ncRNA'}

fail_doc2 = {'_id': '105377262',
             # 'entrezgene': 105377262,
             'name': 'uncharacterized LOC105377262',
             'refseq': {'genomic': ['NC_000004.12', 'NC_018915.2'],
                        'rna': ['XR_938847.2', 'XR_938848.2', 'XR_949449.2', 'XR_949450.2']},
             'symbol': 'LOC105377262',
             'taxid': 9606,
             'type_of_gene': 'ncRNA'}

fail_doc3 = {'_id': '105377262',
             # 'entrezgene': 105377262,
             'name': 'uncharacterized LOC105377262',
             'refseq': {'genomic': ['NC_000004.12', 'NC_018915.2'],
                        'rna': ['XR_938847.2', 'XR_938848.2', 'XR_949449.2', 'XR_949450.2']},
             'symbol': 'LOC105377262',
             'taxid': 9606,
             'type_of_gene': 'ncRNA',
             'uniprot': {'Swiss-Prot': ['a']}}


fail_microbe = {
    "_id": "884205",
    "_score": 13.733018,
    "entrezgene": 884205,
    "locus_tag": "sdf",
    "genomic_pos": {
        "chr": "NC_000117.1",
        "end": 937966,
        "start": 937409,
        "strand": 1
    },
    "name": "50S ribosomal protein L25/general stress protein Ctc",
    "pir": "E71469",
    "refseq": {
        "genomic": "NC_000117.1",
        "protein": "NP_220319.1"
    },
    "taxid": 272561,
    "type_of_gene": "protein-coding",
    "uniprot": {
        "Swiss-Prot": ["O84805", "z"]
    }
}


def test_failing_docs():
    assert_raises(Exception, validate_doc_eukaryotic, fail_doc1)
    assert_raises(Exception, validate_doc_eukaryotic, fail_doc2)
    assert_raises(Exception, validate_doc_eukaryotic, fail_doc3)
    assert_raises(Exception, validate_doc_microbial, fail_microbe)


def test_passing_docs():
    validate_doc_eukaryotic(human_gene_pc)
    validate_doc_eukaryotic(human_gene_minimum)
    validate_doc_microbial(microbe_doc1)


def test_make_reference_timestamp():
    ref = make_ref_source({'id': 'entrez', 'timestamp': '20161204'}, 'P351', '1234')
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
    ref = make_ref_source({'id': 'ensembl', 'release': '86'}, 'P594', '1234')
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
