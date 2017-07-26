import sys
from datetime import datetime

import copy
from cerberus import Validator

from wikidataintegrator import wdi_core, wdi_helpers


def alwayslist(value):
    """If input value if not a list/tuple type, return it as a single value list."""
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return value
    else:
        return [value]


# item for a database
source_items = {'uniprot': 'Q905695',
                'ncbi_gene': 'Q20641742',  # these two are the same
                'entrez': 'Q20641742',
                'ncbi_taxonomy': 'Q13711410',
                'swiss_prot': 'Q2629752',
                'trembl': 'Q22935315',
                'ensembl': 'Q1344256',
                'refseq': 'Q7307074'}

mygene_schema = {
    'entrezgene': {'type': 'integer', 'coerce': int, 'required': True},
    'type_of_gene': {'type': 'string', 'required': True},
    'name': {'type': 'string', 'required': True},
    'symbol': {'type': 'string', 'required': True},
    'taxid': {'type': 'integer', 'required': True, 'coerce': int},
    'SGD': {'type': 'string', 'required': False},
    'HGNC': {'type': 'string', 'required': False},
    'MIM': {'type': 'string', 'required': False},
    'MGI': {'type': 'string', 'required': False},
    'locus_tag': {'type': 'string', 'required': False},
    'RGD': {'type': 'string', 'required': False},
    'FLYBASE': {'type': 'string', 'required': False},
    'WormBase': {'type': 'string', 'required': False},
    'ZFIN': {'type': 'string', 'required': False},
    'ensembl': {
        'type': 'list', 'coerce': alwayslist, 'required': False,
        'schema': {
            'type': 'dict',
            'allow_unknown': True,
            'required': False,
            'schema': {'gene': {'type': 'string'},
                       'protein': {'type': 'list', 'coerce': alwayslist, 'schema': {'type': 'string'}},
                       'transcript': {'type': 'list', 'coerce': alwayslist, 'schema': {'type': 'string'}},
                       },
        }
    },
    'genomic_pos': {
        'type': 'list', 'coerce': alwayslist, 'required': False,
        'schema': {'type': 'dict',
                   'schema': {'chr': {'type': 'string',
                                      'coerce': lambda x: str(x).lower().replace("chr", ""),
                                      'required': True},
                              'start': {'type': 'integer', 'required': True},
                              'end': {'type': 'integer', 'required': True},
                              'strand': {'required': True},
                              'ensemblgene': {'type': 'string', 'required': False}}}},
    'genomic_pos_hg19': {
        'type': 'list', 'coerce': alwayslist, 'required': False,
        'schema': {'type': 'dict',
                   'schema': {'chr': {'type': 'string',
                                      'coerce': lambda x: str(x).lower().replace("chr", ""),
                                      'required': True},
                              'start': {'type': 'integer', 'required': True},
                              'end': {'type': 'integer', 'required': True},
                              'strand': {'required': True},
                              'ensemblgene': {'type': 'string', 'required': False}}}},
    'uniprot': {
        'type': 'dict',
        'required': False,
        'schema': {
            'Swiss-Prot': {'required': False, 'type': 'string'},
            'TrEMBL': {'required': False, 'type': 'list', 'coerce': alwayslist, 'schema': {'type': 'string'}}
        }
    },
    'homologene': {
        'type': 'dict',
        'required': False,
        'allow_unknown': True,
        'schema': {'id': {'required': True, 'type': 'string', 'coerce': str}}},
    'refseq': {
        'type': 'dict',
        'required': False,
        'allow_unknown': True,
        'schema': {
            'protein': {'type': 'list',
                        # remove version numbers
                        'coerce': lambda ss: [s.rsplit(".")[0] for s in alwayslist(ss)],
                        'schema': {'type': 'string'}},
            'rna': {'type': 'list',
                    # remove version numbers
                    'coerce': lambda ss: [s.rsplit(".")[0] for s in alwayslist(ss)],
                    'schema': {'type': 'string'}},
        }
    },
    'alias': {'type': 'list', 'required': False, 'coerce': alwayslist, 'schema': {'type': 'string'}},
    'other_names': {'type': 'list', 'required': False, 'coerce': alwayslist, 'schema': {'type': 'string'}},

}
mygene_schema_microbe = copy.deepcopy(mygene_schema)
mygene_schema_microbe['genomic_pos']['required'] = True
mygene_schema_microbe['locus_tag']['required'] = True
mygene_schema_microbe['uniprot']['required'] = True
mygene_schema_microbe['genomic_pos']['maxlength'] = 1
mygene_schema_microbe['genomic_pos']['minlength'] = 1


def validate_docs(docs, doc_type, external_id_prop):
    assert doc_type in {'eukaryotic', 'microbial'}
    if doc_type == "microbial":
        f = validate_doc_microbial
    else:
        f = validate_doc_eukaryotic
    for doc in docs:
        try:
            doc = f(doc)
        except ValueError as e:
            print(e)
            wdi_core.WDItemEngine.log("WARNING",
                                      wdi_helpers.format_msg(doc['_id'], external_id_prop, None, str(e), type(e)))
            continue
        yield doc


def validate_doc_eukaryotic(d):
    v = Validator(mygene_schema, allow_unknown=True)
    v.validate(d)
    if v.errors:
        raise ValueError(v.errors)
    return v.document


def validate_doc_microbial(d):
    v = Validator(mygene_schema_microbe, allow_unknown=True)
    v.validate(d)
    if v.errors:
        raise ValueError(v.errors)
    return v.document


def parse_mygene_src_version(d):
    """
    Parse source information. Make sure they are annotated as releases or with a timestamp
    d: looks like: {"ensembl" : 84, "cpdb" : 31, "netaffy" : "na35", "ucsc" : "20160620", .. }
    :return: dict, looks likes:
        {'ensembl': {'id': 'ensembl', 'release': '87'},
        'entrez': {'id': 'entrez', 'timestamp': '20161204'}}
    """
    d2 = {}
    for source, version in d.items():
        if source in {'ensembl', 'refseq'}:
            d2[source] = {'id': source, 'release': str(version)}
        elif source in {'uniprot', 'entrez', 'ucsc'}:
            d2[source] = {'id': source, 'timestamp': str(version)}
    return d2


def tag_mygene_docs(docs, metadata):
    """
    The purpose of this to is to tag each field with its source. This is hardcoded/defined here for now.
    Until it comes from mygene.info itself
    :param docs: list of dicts. Keys not in key_source are removed!!
    :param metadata: looks like: {"ensembl" : 84, "cpdb" : 31, "netaffy" : "na35", "ucsc" : "20160620", .. }
    :return:
    """
    source_dict = parse_mygene_src_version(metadata)
    key_source = {'SGD': 'entrez',
                  'HGNC': 'entrez',
                  'MIM': 'entrez',
                  'FLYBASE': 'entrez',
                  'WormBase': 'entrez',
                  'ZFIN': 'entrez',
                  'RGD': 'entrez',
                  'MGI': 'entrez',
                  'exons': 'ucsc',
                  'ensembl': 'ensembl',
                  'entrezgene': 'entrez',
                  'genomic_pos': None,
                  'genomic_pos_hg19': None,
                  'locus_tag': 'entrez',
                  'name': 'entrez',
                  'symbol': 'entrez',
                  'taxid': 'entrez',
                  'type_of_gene': 'entrez',
                  'refseq': 'entrez',
                  'uniprot': 'uniprot',
                  'homologene': 'entrez',
                  'other_names': 'entrez',
                  'alias': 'entrez'
                  }
    # todo: automate getting this list of ensembl taxids
    # http://uswest.ensembl.org/info/about/species.html
    ensembl_taxids = [4932, 6239, 7227, 7719, 7757, 7897, 7918, 7955, 7994, 8049, 8083, 8090, 8128, 8364, 8479, 8839,
                      9031, 9103, 9258, 9305, 9315, 9358, 9361, 9365, 9365, 9371, 9483, 9541, 9544, 9555, 9557, 9595,
                      9598, 9598, 9601, 9606, 9615, 9646, 9669, 9685, 9739, 9739, 9755, 9785, 9796, 9813, 9823, 9913,
                      9940, 9978, 9978, 9986, 10020, 10029, 10090, 10116, 10141, 10181, 13146, 13616, 13735, 28377,
                      30538, 30608, 30611, 31033, 31033, 37347, 39432, 42254, 42254, 43179, 48698, 51511, 59463, 59729,
                      59894, 60711, 61853, 69293, 73337, 79684, 99883, 132908, 1230840, 1868482]

    for doc in docs:
        if doc['taxid'] in ensembl_taxids:
            key_source['genomic_pos'] = 'ensembl'
            key_source['genomic_pos_hg19'] = 'ensembl'
        else:
            key_source['genomic_pos'] = 'entrez'
            key_source['genomic_pos_hg19'] = 'entrez'

        tagged_doc = {k: {'@value': v, '@source': source_dict[key_source[k]]} for k, v in doc.items() if
                      k in key_source}

        yield tagged_doc


def make_ref_source(source_doc, id_prop, identifier, login=None):
    """
    Reference is made up of:
    stated_in: if the source has a release #:
        release edition
        else, stated in the source
    link to id: link to identifier in source
    retrieved: only if source has no release #
    login: must be passed if you want to be able to create new release items

    :param source_doc:
    Example source_doc = {'_id': 'uniprot', 'timestamp': '20161006'}
    or source_doc = {'_id': 'ensembl', 'release': '86'}
    :param id_prop:
    :param identifier:
    :return:
    """
    source = source_doc['id']
    if source not in source_items:
        raise ValueError("Unknown source for reference creation: {}".format(source))
    assert id_prop.startswith("P")

    link_to_id = wdi_core.WDString(value=str(identifier), prop_nr=id_prop, is_reference=True)

    if "release" in source_doc:
        source_doc['release'] = str(source_doc['release'])
        title = "{} Release {}".format(source_doc['id'], source_doc['release'])
        description = "Release {} of {}".format(source_doc['release'], source_doc['id'])
        edition_of_wdid = source_items[source_doc['id']]
        release = wdi_helpers.Release(title, description, source_doc['release'],
                                      edition_of_wdid=edition_of_wdid).get_or_create(login)

        stated_in = wdi_core.WDItemID(value=release, prop_nr='P248', is_reference=True)
        reference = [stated_in, link_to_id]
    else:
        date_string = source_doc['timestamp']
        retrieved = datetime.strptime(date_string, "%Y%m%d")
        stated_in = wdi_core.WDItemID(value=source_items[source], prop_nr='P248', is_reference=True)
        retrieved = wdi_core.WDTime(retrieved.strftime('+%Y-%m-%dT00:00:00Z'), prop_nr='P813', is_reference=True)
        reference = [stated_in, retrieved, link_to_id]
    return reference


def make_reference(source, id_prop, identifier, retrieved):
    reference = [
        wdi_core.WDItemID(value=source_items[source], prop_nr='P248', is_reference=True),  # stated in
        wdi_core.WDString(value=str(identifier), prop_nr=id_prop, is_reference=True),  # Link to ID
        wdi_core.WDTime(retrieved.strftime('+%Y-%m-%dT00:00:00Z'), prop_nr='P813', is_reference=True)]
    return reference
