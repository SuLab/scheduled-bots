import itertools
import requests
from cachetools import cached, TTLCache
CACHE_SIZE = 10000
CACHE_TIMEOUT_SEC = 300  # 5 min

def get_values(pid, values):
    # todo: integrate this into wdi_helpers
    value_quotes = '"' + '" "'.join(map(str, values)) + '"'
    s = """select * where {
          values ?x {**value_quotes**}
          ?item wdt:**pid** ?x
        }""".replace("**value_quotes**", value_quotes).replace("**pid**", pid)
    params = {'query': s, 'format': 'json'}
    request = requests.post('https://query.wikidata.org/sparql', data=params)
    request.raise_for_status()
    results = request.json()['results']['bindings']
    dl = [{k: v['value'] for k, v in item.items()} for item in results]
    return {x['x']: x['item'].replace("http://www.wikidata.org/entity/", "") for x in dl}


def login_to_wikidata(USER, PASS):
    baseurl = 'https://www.wikidata.org/w/'
    # Login request
    payload = {'action': 'query', 'format': 'json', 'utf8': '', 'meta': 'tokens', 'type': 'login'}
    r1 = requests.post(baseurl + 'api.php', data=payload)
    # login confirm
    login_token = r1.json()['query']['tokens']['logintoken']
    payload = {'action': 'login', 'format': 'json', 'utf8': '', 'lgname': USER, 'lgpassword': PASS,
               'lgtoken': login_token}
    r2 = requests.post(baseurl + 'api.php', data=payload, cookies=r1.cookies)
    # get edit token2
    params3 = '?format=json&action=query&meta=tokens&continue='
    r3 = requests.get(baseurl + 'api.php' + params3, cookies=r2.cookies)
    edit_token = r3.json()['query']['tokens']['csrftoken']
    edit_cookie = r2.cookies.copy()
    edit_cookie.update(r3.cookies)
    return edit_token, edit_cookie


def pd_to_table(df):
    # quick n dirty pandas DataFrame to mediawikitable converter
    """{| border="1" class="dataframe"
        |- style="text-align: right;"
        !
        ! Article Name!! wikidata ID!! InterPro Items!! InterPro WDIDs!! About Gene!! Done
        |-
        ! 0
        |[[:en:Tetratricopeptide|Tetratricopeptide]]||[[Q7706768]]||[www.ebi.ac.uk/interpro/entry/IPR001440 IPR001440]||[[Q24779822]]||False||False
        |-
        ! 1
        |[[:en:UDP-N-acetylglucosamine 2-epimerase|UDP-N-acetylglucosamine 2-epimerase]]||[[Q13411653]]||[www.ebi.ac.uk/interpro/entry/IPR003331 IPR003331]||[[Q24721922]]||False||False
        |}
    """
    out = "{| border='1' class='wikitable sortable table-yes table-no' style='text-align: left;'\n!\n"
    out += '!'.join(['! {}'.format(x) for x in list(df.columns)])
    for record in df.to_records():
        record = list(record)
        record = list(map(lambda x: x.replace("\r\n", "<br>") if isinstance(x, str) else x, record))
        out += "\n|-\n"
        out += "! " + str(record[0]) + '\n'
        out += '|'.join(['|{}'.format(x) for x in record[1:]])
    out += "\n|}"
    return out


def execute_sparql_query(query, prefix=None, endpoint='https://query.wikidata.org/sparql',
                         user_agent='wikidatasparqlexamples: https://github.com/SuLab/wikidatasparqlexamples'):
    wd_standard_prefix = '''
        PREFIX wd: <http://www.wikidata.org/entity/>
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        PREFIX wikibase: <http://wikiba.se/ontology#>
        PREFIX p: <http://www.wikidata.org/prop/>
        PREFIX v: <http://www.wikidata.org/prop/statement/>
        PREFIX q: <http://www.wikidata.org/prop/qualifier/>
        PREFIX ps: <http://www.wikidata.org/prop/statement/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    '''
    if not prefix:
        prefix = wd_standard_prefix
    params = {'query': prefix + '\n' + query,
              'format': 'json'}
    headers = {'Accept': 'application/sparql-results+json',
               'User-Agent': user_agent}
    response = requests.get(endpoint, params=params, headers=headers)
    response.raise_for_status()
    return response.json()


def grouper(n, iterable):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


def make_deletion_templates(qids, title, reason):
    s = '\n=={}==\n'.format(title)
    for group in grouper(90, list(qids)):
        del_template = "{{subst:Rfd group | {q} | reason = {reason} }}\n".replace("{q}", '|'.join(group)).replace(
            "{reason}", reason)
        s += del_template
    return s


def create_rfd(s: str, WDUSER, WDPASS):
    # post the request for deletion on wikidata
    edit_token, edit_cookie = login_to_wikidata(WDUSER, WDPASS)
    data = {'action': 'edit', 'title': 'Wikidata:Requests_for_deletions',
            'appendtext': s, 'format': 'json', 'token': edit_token}
    r = requests.post("https://www.wikidata.org/w/api.php", data=data, cookies=edit_cookie)
    r.raise_for_status()
    print(r.json())


def clean_description(s: str):
    # adhere to style guidelines: https://www.wikidata.org/wiki/Help:Description
    # Remove starting with "A ", remove final period, replace underscores with spaces

    s = s.replace("_", " ")
    if "." in s[:-1]:
        # if s has a period in it anywhere but the last line, don't attempt to perform these actions
        return s
    if s.startswith("A "):
        s = s[2:]
    elif s.startswith("An "):
        s = s[3:]
    if s.endswith("."):
        s = s[:-1]
    return s

@cached(TTLCache(CACHE_SIZE, CACHE_TIMEOUT_SEC))
def getConceptLabel(qid):
    return getConceptLabels((qid,))[qid]


@cached(TTLCache(CACHE_SIZE, CACHE_TIMEOUT_SEC))
def getConceptLabels(qids):
    qids = "|".join({qid.replace("wd:", "") if qid.startswith("wd:") else qid for qid in qids})
    params = {'action': 'wbgetentities', 'ids': qids, 'languages': 'en', 'format': 'json', 'props': 'labels'}
    r = requests.get("https://www.wikidata.org/w/api.php", params=params)
    print(r.url)
    r.raise_for_status()
    wd = r.json()['entities']
    return {k: v['labels']['en']['value'] for k, v in wd.items()}