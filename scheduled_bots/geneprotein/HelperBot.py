from datetime import datetime

import requests
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

def check_record(record):
    # only one genomic position
    assert 'genomic_pos' in record and isinstance(record['genomic_pos'], dict)
    if 'genomic_pos_hg19' in record:
        assert isinstance(record['genomic_pos_hg19'], dict)

    # required keys and sub keys
    required = {'entrezgene': None,
                'ensembl': {'gene', 'transcript', 'protein'},
                'refseq': {'rna', 'protein'},  # not using refseq['genomic']
                'type_of_gene': None,
                'name': None,
                'genomic_pos': {'start', 'end', 'chr', 'strand'}
                }
    for key,value in required.items():
        assert key in record, "{} not in record".format(key)
        if hasattr(value, "__iter__"):
            for sub_key in required[key]:
                assert sub_key in record[key], "{} not in {}".format(sub_key, key)

    # make sure certain fields are lists
    if not isinstance(record['ensembl']['transcript'], list):
        record['ensembl']['transcript'] = [record['ensembl']['transcript']]
    if not isinstance(record['ensembl']['protein'], list):
        record['ensembl']['protein'] = [record['ensembl']['protein']]
    if not isinstance(record['refseq']['rna'], list):
        record['refseq']['rna'] = [record['refseq']['rna']]
    if not isinstance(record['refseq']['protein'], list):
        record['refseq']['protein'] = [record['refseq']['protein']]

    return record


def get_mygene_src_version():
    """
    Get source information from mygene. Make sure they are annotated as releases or with a timestamp
    :return: dict, looks likes:
        {'ensembl': {'id': 'ensembl', 'release': '87'},
        'entrez': {'id': 'entrez', 'timestamp': '20161204'}}
    """
    d = requests.get("http://mygene.info/v3/metadata").json()
    d2 = {}
    for source, version in d['src_version'].items():
        if source in {'ensembl', 'refseq'}:
            d2[source] = {'id': source, 'release': str(version)}
        elif source in {'uniprot', 'entrez', 'ucsc'}:
            d2[source] = {'id': source, 'timestamp': str(version)}
    return d2


def tag_mygene_docs(docs):
    """
    The purpose of this to is to tag each field with its source. This is hardcoded/defined here for now.
    Until it comes from mygene.info itself
    :param docs: list of dicts. Keys not in key_source are removed!!
    :return:
    """
    source_dict = get_mygene_src_version()
    key_source = {'SGD': 'entrez',
                  'HGNC': 'entrez',
                  'MIM': 'entrez',
                  'MGI': 'entrez',
                  'exons': 'ucsc',
                  'ensembl': 'ensembl',
                  'entrezgene': 'entrez',
                  'genomic_pos': None,
                  'locus_tag': 'entrez',
                  'name': 'entrez',
                  'symbol': 'entrez',
                  'taxid': 'entrez',
                  'type_of_gene': 'entrez',
                  'refseq': 'entrez',
                  'uniprot': 'uniprot'
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
        else:
            key_source['genomic_pos'] = 'entrez'

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

