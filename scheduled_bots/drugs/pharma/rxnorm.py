from functools import lru_cache

import requests


@lru_cache(maxsize=100000)
def get_label_from_rxcui(rxcui):
    url = "https://rxnav.nlm.nih.gov/REST/rxcui/{}/properties.json".format(rxcui)
    d = requests.get(url).json()
    if d:
        return d['properties']['name']


@lru_cache(maxsize=100000)
def get_rxcui_brandname(rxcui):
    url = "https://rxnav.nlm.nih.gov/REST/rxcui/{}/related.json?tty=BN".format(rxcui)
    d = requests.get(url).json()
    ingredients = {x['tty']: x.get('conceptProperties', []) for x in d['relatedGroup']['conceptGroup'] if
                   x['tty'] in {'BN'}}
    if len(ingredients['BN']):
        return ingredients['BN'][0]['rxcui']


@lru_cache(maxsize=100000)
def get_rxcui_ingredient(rxcui, related=None):
    """
    Get from ingredient/dose/form to compound
    example: rxcui: 1442407 (Camphor 48 MG/ML / Eucalyptus oil 12 MG/ML / Menthol 26 MG/ML Topical Cream)
    to: 691178 (Camphor / Eucalyptus oil / Menthol)
    https://rxnav.nlm.nih.gov/REST/rxcui/1442407/allrelated.json
    http://bioportal.bioontology.org/ontologies/RXNORM?p=classes&conceptid=1442407

    Look for MIN, PIN, or IN
    types: https://www.nlm.nih.gov/research/umls/rxnorm/docs/2015/appendix5.html
    api doc: https://rxnav.nlm.nih.gov/RxNormAPIs.html#uLink=RxNorm_REST_getAllRelatedInfo

    if related is given (as output of get_rxcui_related, use that instead of making an api call)
    :param rxcui:
    :return:
    """
    if not related:
        related = get_rxcui_related(rxcui)
    if related['MIN']:
        return related['MIN']
    elif related['PIN']:
        return related['PIN']
    elif related['IN']:
        return related['IN']
    else:
        return None


@lru_cache(maxsize=100000)
def get_rxcui_related(rxcui):
    url = "https://rxnav.nlm.nih.gov/REST/rxcui/{}/allrelated.json".format(rxcui)
    d = requests.get(url).json()
    related = {x['tty']: x.get('conceptProperties', []) for x in
               d['allRelatedGroup']['conceptGroup']}
    return related


get_rxcui_ingredient(403878)
get_rxcui_brandname(403878)
get_label_from_rxcui(614534)

get_rxcui_ingredient(497184)
