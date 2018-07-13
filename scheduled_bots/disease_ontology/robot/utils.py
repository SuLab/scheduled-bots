from bs4 import BeautifulSoup
import requests
import atexit
import pickle

BIOPORTAL_KEY = "a1ac23bb-23cb-44cf-bf5e-bcdd7446ef37"


def persist_cache_to_disk(filename):
    # http://tohyongcheng.github.io/python/2016/06/07/persisting-a-cache-in-python-to-disk.html
    def decorator(original_func):
        try:
            cache = pickle.load(open(filename, 'rb'))
        except (IOError, ValueError) as e:
            cache = {}

        atexit.register(lambda: pickle.dump(cache, open(filename, "wb")))

        def new_func(*args):
            if tuple(args) not in cache:
                cache[tuple(args)] = original_func(*args)
            return cache[args]

        return new_func

    return decorator


@persist_cache_to_disk('gard.pkl')
def get_gard_info(gard_id):
    # silly web scraping
    url = "https://rarediseases.info.nih.gov/diseases/{}/index".format(gard_id)
    p = BeautifulSoup(requests.get(url).text, "lxml")
    title = p.find_all("h1")[0].text.strip()
    try:
        synonyms = p.find(id="diseaseSynonyms").find('span', {'class': "complete"}).text
    except Exception:
        synonyms = ''
    d = {'gard_label': title, 'gard_descr': '', 'gard_synonyms': synonyms}
    return d


@persist_cache_to_disk('mesh.pkl')
def get_mesh_info(mesh_id):
    url = "http://data.bioontology.org/ontologies/MESH/classes/http%3A%2F%2Fpurl.bioontology.org%2Fontology%2FMESH%2F{}"
    d = requests.get(url.format(mesh_id), params={'apikey': BIOPORTAL_KEY}).json()
    if "errors" in d:
        return {'mesh_label': '', 'mesh_descr': '', 'mesh_synonyms': ''}
    d = {'mesh_label': d['prefLabel'], 'mesh_descr': d['definition'], 'mesh_synonyms': ";".join(d['synonym'])}
    d['mesh_descr'] = d['mesh_descr'][0] if d['mesh_descr'] else ''
    return d


@persist_cache_to_disk('ordo.pkl')
def get_ordo_info(mesh_id):
    url = "https://www.ebi.ac.uk/ols/api/ontologies/ordo/terms?iri=http://www.orpha.net/ORDO/Orphanet_{}"
    d = requests.get(url.format(mesh_id)).json()
    try:
        d = d['_embedded']['terms'][0]
        d = {'ordo_label': d['label'], 'ordo_descr': d['description'], 'ordo_synonyms': ";".join(d['synonyms'])}
        d['ordo_descr'] = d['ordo_descr'][0] if d['ordo_descr'] else ''
    except Exception:
        return {'ordo_label': '', 'ordo_descr': '', 'ordo_synonyms': ''}
    return d


@persist_cache_to_disk('omim.pkl')
def get_omim_info(omim_id):
    url = "https://api.omim.org/api/entry?mimNumber={}".format(omim_id)
    params = {'apiKey': 'YusepqJtQDuqSPctv6tmVQ', 'format': 'json'}
    try:
        d = requests.get(url, params=params).json()['omim']['entryList'][0]['entry']
    except Exception:
        return {'omim_label': '', 'omim_descr': '', 'omim_synonyms': '', 'omim_prefix': ''}
    print(d)
    d = {'omim_label': d['titles']['preferredTitle'], 'omim_descr': '',
         'omim_synonyms': d['titles']['alternativeTitles'].replace("\n", "") if 'alternativeTitles' in d[
             'titles'] else '',
         'omim_prefix': d.get('prefix', '')}
    return d
