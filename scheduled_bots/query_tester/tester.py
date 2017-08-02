import os
from urllib.parse import quote_plus
import pandas as pd
from scheduled_bots.query_tester import validators
import requests
import mwparserfromhell
from scheduled_bots.utils import pd_to_table, login_to_wikidata, execute_sparql_query

try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")


class SPARQLTester:
    def __init__(self, url=None):
        self.url = url if url else "User:ProteinBoxBot/Maintenance_Queries"
        self.tests = []
        self.get_queries()

    def get_queries(self):
        baseurl = 'https://www.wikidata.org/w/'
        params = {'action': 'query', 'prop': 'revisions', 'rvprop': 'content', 'format': 'json',
                  'titles': self.url}
        r = requests.get(baseurl + "api.php", params=params)
        d = r.json()
        page = list(d['query']['pages'])[0]
        page_text = d['query']['pages'][page]['revisions'][0]['*']

        wikicode = mwparserfromhell.parse(page_text)
        templates = wikicode.filter_templates(recursive=False)

        for template in templates:
            query = str(template.get("query").value).replace("{{!}}", "|")
            v = str(template.get("validator").value) if "validator" in template else None
            name = str(template.get("name").value) if "name" in template else ''
            q = SPARQLTest(query, v, name)
            self.tests.append(q)

    def run(self):
        for test in self.tests:
            test.test()

    def make_result_table(self):
        r = []
        for test in self.tests:
            q_results = [{k: "[[{}]]".format(
                v['value'].replace("http://www.wikidata.org/entity/", "")) if "http://www.wikidata.org/entity/" in v[
                'value'] else v['value'] for k, v in result.items()} for result in test.result]
            append = ""
            if len(q_results) > 3:
                q_results = q_results[:3]
                append = "\n....\n"
            qr_str = "\n".join(
                [", ".join(":".join([k, v]) for k, v in result.items()) for result in q_results]) + append

            rr = {'Query Name': test.name,
                  # 'Validator': test.validator_class.__name__,
                  'Validator Description': test.validator.description,
                  'Test Status': "Pass" if test.validator.success else "Fail",
                  'Query Result': qr_str,
                  "Number of Results": len(test.result),
                  # 'Result Message': test.validator.result_message,
                  'URL': test.create_url()}
            r.append(rr)
        df = pd.DataFrame(r)
        return df

    def make_wikimedia_table(self):
        df = self.make_result_table()
        df.URL = df.URL.apply(lambda x: "[{} Run]".format(x))
        return pd_to_table(df)


class SPARQLTest:
    def __init__(self, query, validator, name=''):
        self.s = query
        self.name = name
        self.result = {}
        self.validator = validator
        self.validator_class = None
        if self.validator and hasattr(validators, self.validator):
            self.validator_class = getattr(validators, self.validator)

    def create_url(self):
        url = "https://query.wikidata.org/#"
        url += quote_plus(self.s)
        url = url.replace("+", "%20")
        return url

    def test(self):
        print("running: {}".format(self.name))
        if not self.validator_class:
            print("{}: no validator specified".format(self.name))
            self.validator_class = validators.NoValidator

        result = execute_sparql_query(self.s)
        assert 'results' in result and 'bindings' in result['results']
        self.result = result['results']['bindings']
        self.validator = self.validator_class()
        self.validator.validate(self.result)

    def print_results(self):
        s = """
Query: {}
Validator: {} ({})
PASS?: {}
-----------
SPARQL: {}
-----------
Result: {}
        """.format(self.name, self.validator_class.__name__,
                   self.validator.description, self.validator.success, self.s, self.result)
        print(s)

    def get_results(self):
        df = pd.DataFrame(self.result)
        return df


if __name__ == '__main__':
    t = SPARQLTester()
    t.tests = t.tests
    t.run()
    mwtable = t.make_wikimedia_table()
    print(mwtable)
    edit_token, edit_cookie = login_to_wikidata(WDUSER, WDPASS)
    data = {'action': 'edit', 'title': 'User:ProteinBoxBot/Maintenance_Query_Results',
            'text': mwtable, 'format': 'json', 'token': edit_token}
    r = requests.post("https://www.wikidata.org/w/api.php", data=data, cookies=edit_cookie)
    r.raise_for_status()
