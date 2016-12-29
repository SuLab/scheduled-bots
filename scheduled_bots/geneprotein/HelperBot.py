import sys
import traceback
from datetime import datetime

from wikidataintegrator import wdi_core, wdi_helpers

# item for a database
source_items = {'uniprot': 'Q905695',
                'ncbi_gene': 'Q20641742',  # these two are the same?  --v
                'entrez': 'Q20641742',
                'ncbi_taxonomy': 'Q13711410',
                'swiss_prot': 'Q2629752',
                'trembl': 'Q22935315',
                'ensembl': 'Q1344256',
                'refseq': 'Q7307074'}


def validate_docs(docs, doc_type, external_id_prop):
    assert doc_type in {'gene', 'protein'}
    for doc in docs:
        try:
            doc = validate_doc(doc, doc_type)
        except AssertionError as e:
            exc_info = sys.exc_info()
            traceback.print_exception(*exc_info)
            wdi_core.WDItemEngine.log("ERROR",
                                      wdi_helpers.format_msg(doc['_id'], external_id_prop, None, str(e), type(e)))
            continue
        yield doc


def alwayslist(value):
    """If input value if not a list/tuple type, return it as a single value list."""
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return value
    else:
        return [value]


def validate_doc(d, doc_type):
    """
    Check fields in mygene doc. and neccessary transformations
    Remove version numbers from genomic/transcriptomic seq IDs
    :param d:
    :param doc_type: is one of 'gene' or 'protein'
    :return:
    """
    assert doc_type in {'gene', 'protein'}

    if doc_type == 'gene':
        # make sure only one genomic position and its a dict
        assert 'genomic_pos' in d and isinstance(d['genomic_pos'], dict), 'genomic_pos'
        if 'genomic_pos_hg19' in d:
            assert isinstance(d['genomic_pos_hg19'], dict), 'genomic_pos_hg19'

    # check existence required keys and sub keys
    def check_keys_subkeys(required):
        for key, value in required.items():
            assert key in d, "{} not in record".format(key)
            if hasattr(value, "__iter__"):
                for sub_key in required[key]:
                    assert sub_key in d[key], "{} not in {}".format(sub_key, key)

    required = {'entrezgene': None,
                'type_of_gene': None,
                'name': None }
    required_gene = {'ensembl': {'gene', 'transcript'},
                     'refseq': {'rna'},  # not using refseq['genomic']
                     'genomic_pos': {'start', 'end', 'chr', 'strand'} }
    required_protein = {'ensembl': {'protein'},
                        'refseq': {'protein'},
                        'uniprot': {'Swiss-Prot'}}

    check_keys_subkeys(required)
    if doc_type == "gene":
        check_keys_subkeys(required_gene)
    if doc_type == "protein":
        check_keys_subkeys(required_protein)

    # make sure these fields are lists
    if doc_type == "gene":
        d['ensembl']['transcript'] = alwayslist(d['ensembl']['transcript'])
        d['refseq']['rna'] = alwayslist(d['refseq']['rna'])
    if doc_type == "protein":
        d['ensembl']['protein'] = alwayslist(d['ensembl']['protein'])
        d['refseq']['protein'] = alwayslist(d['refseq']['protein'])
    if 'alias' in d:
        d['alias'] = alwayslist(d['alias'])

    # make sure these fields are not lists
    if doc_type == "gene":
        assert isinstance(d['ensembl']['gene'], str), "incorrect type: doc['ensembl']['gene']"
        assert isinstance(d['entrezgene'], (int, str)), "incorrect type: doc['entrezgene']"
    if doc_type == "protein":
        assert isinstance(d['uniprot']['Swiss-Prot'], str), "incorrect type: doc['uniprot']['Swiss-Prot']"
    # assert isinstance(record['refseq']['genomic'], str)  # this isn't used

    # check types of optional and required fields
    fields = {'SGD': str, 'HGNC': str, 'MIM': str, 'MGI': str, 'locus_tag': str, 'symbol': str, 'taxid': int,
              'type_of_gene': str, 'name': str}
    for field, field_type in fields.items():
        if field in d:
            assert isinstance(d[field], field_type), "incorrect type: {}".format(field)

    # check optional dict fields
    if doc_type == "protein":
        d['uniprot']['Swiss-Prot'] = alwayslist(d['uniprot']['Swiss-Prot'])

    if doc_type == "gene" and 'homologene' in d:
        assert "id" in d['homologene'] and isinstance(d['homologene']['id'], (int, str)), "doc['homologene']['id']"

    # remove version numbers (these are always lists)
    remove_version = lambda ss: [s.rsplit(".")[0] if "." in s else s for s in ss]
    if doc_type == "gene":
        d['refseq']['rna'] = remove_version(d['refseq']['rna'])
    if doc_type == "protein":
        d['refseq']['protein'] = remove_version(d['refseq']['protein'])

    return d


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
                  }
    # todo: automate getting this list of ensembl taxids
    # http://uswest.ensembl.org/info/about/species.html
    ensembl_taxids = [1230840, 30538, 48698, 28377, 9361, 13146, 30611, 7719, 51511, 6239, 9685, 7994, 9031, 9598, 9598,
                      10029, 13735, 8049, 7897, 9913, 9541, 9615, 9739, 9739, 8839, 9785, 9669, 59894, 7227, 31033,
                      31033, 61853, 9595, 10141, 9557, 9365, 9365, 9796, 9606, 9813, 10020, 7757, 9371, 9544, 9483,
                      8090, 132908, 59463, 10090, 30608, 10181, 9555, 13616, 9601, 8479, 9646, 9823, 9978, 9978, 8083,
                      9258, 79684, 9986, 10116, 73337, 4932, 9940, 42254, 42254, 9358, 9755, 7918, 43179, 39432, 69293,
                      1868482, 9305, 99883, 8128, 37347, 9103, 60711, 9315, 8364, 59729, 7955]

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
