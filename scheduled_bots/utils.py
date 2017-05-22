import requests
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