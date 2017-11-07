from scheduled_bots.utils import getConceptLabels
from wikidataintegrator import wdi_core
from wikidataintegrator.wdi_core import WDItemEngine
from wikidataintegrator.wdi_helpers import try_write

PROPS = {
    "subclass of": "P279",
    "has part": "P527",
}

# chromosomes
CHROMOSOME = dict()
CHROMOSOME['1'] = "Q430258"
CHROMOSOME['2'] = "Q638893"
CHROMOSOME['3'] = "Q668633"
CHROMOSOME['4'] = "Q836605"
CHROMOSOME['5'] = "Q840741"
CHROMOSOME['6'] = "Q540857"
CHROMOSOME['7'] = "Q657319"
CHROMOSOME['8'] = "Q572848"
CHROMOSOME['9'] = "Q840604"
CHROMOSOME['10'] = "Q840737"
CHROMOSOME['11'] = "Q847096"
CHROMOSOME['12'] = "Q847102"
CHROMOSOME['13'] = "Q840734"
CHROMOSOME['14'] = "Q138955"
CHROMOSOME['15'] = "Q765245"
CHROMOSOME['16'] = "Q742870"
CHROMOSOME['17'] = "Q220677"
CHROMOSOME['18'] = "Q780468"
CHROMOSOME['19'] = "Q510786"
CHROMOSOME['20'] = "Q666752"
CHROMOSOME['21'] = "Q753218"
CHROMOSOME['22'] = "Q753805"
CHROMOSOME['22'] = "Q753805"
CHROMOSOME['X'] = "Q61333"
CHROMOSOME['Y'] = "Q202771"
CHROMOSOME["MT"] = "Q27075"

# CIViC evidence scores
EVIDENCE_LEVEL = dict()
EVIDENCE_LEVEL["A"] = "Q36805652"
EVIDENCE_LEVEL["B"] = "Q36806012"
EVIDENCE_LEVEL["C"] = "Q36799701"
EVIDENCE_LEVEL["D"] = "Q36806470"
EVIDENCE_LEVEL["E"] = "Q36811327"

# CIViC trust ratings
TRUST_RATING = dict()
TRUST_RATING["1"] = "Q28045396"
TRUST_RATING["2"] = "Q28045397"
TRUST_RATING["3"] = "Q28045398"
TRUST_RATING["4"] = "Q28045383"
TRUST_RATING["5"] = "Q28045399"


IGNORE_SYNONYMS = [
    "AMPLIFICATION",
    "EXPRESSION",
    "DELETION",
    "LOSS",
    "LOSS-OF-FUNCTION",
    "MUTATION",
    "NUCLEAR EXPRESSION",
    "OVEREXPRESSION",
    "UNDEREXPRESSION",
    "3\' UTR MUTATION",
    "BIALLELIC INACTIVATION",
    "EWSR1-FLI1",
    "EXON 12 MUTATION",
    "EXON 9 MUTATION",
    "FRAMESHIFT TRUNCATION",
    "G12",
    "G12/G13",
    "G13D",
    "METHYLATION",
    "PHOSPHORYLATION",
    "PROMOTER HYPERMETHYLATION",
    "PROMOTER METHYLATION",
    "SERUM LEVELS",
    "TMPRSS2-ERG",
    "TRUNCATING MUTATION",
]

class DrugCombo:
    """
    Interact with drug combination therapy items in wikidata
    example: https://www.wikidata.org/wiki/Q38159991
    """
    # `combo` is a frozenset of qids of the components of the drug combination therapy
    # `qid` is the qid of the combination item
    combo_qid = None
    qid_combo = None

    @classmethod
    def get_existing(cls):
        # get existing combinations:
        query_str = """SELECT ?item ?itemLabel (GROUP_CONCAT(?part; separator=";") as ?f) WHERE {
          ?item wdt:P527 ?part .
          ?item wdt:P31 wd:Q1304270 .
          SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
        } GROUP BY ?item ?itemLabel"""
        results = WDItemEngine.execute_sparql_query(query_str)['results']['bindings']
        qid_combo = {x['item']['value'].replace("http://www.wikidata.org/entity/", ""): frozenset(
            [y.replace("http://www.wikidata.org/entity/", "") for y in x['f']['value'].split(";")]) for x in results}
        combo_qid = {v: k for k, v in qid_combo.items()}
        assert len(combo_qid) == len(qid_combo)
        cls.combo_qid = combo_qid
        cls.qid_combo = qid_combo

    def __init__(self, component_qids):
        if not self.combo_qid:
            self.get_existing()
        self.component_qids = frozenset(component_qids)

    def get_or_create(self, login=None):
        if self.component_qids in self.combo_qid:
            return self.combo_qid[self.component_qids]
        if not login:
            print("Login is required to create item")
            return None
        return self.create(login)

    def create(self, login):
        # get names of components
        labels = getConceptLabels(self.component_qids)

        name = " / ".join(labels.values()) + " combination therapy"
        description = "combination therapy"

        # has part
        s = [wdi_core.WDItemID(x, PROPS['has part']) for x in self.component_qids]
        # instance of combination therapy
        s.append(wdi_core.WDItemID("Q1304270", PROPS['instance of']))

        item = wdi_core.WDItemEngine(item_name=name, data=s, domain="asdf")
        item.set_label(name)
        item.set_description(description)
        return try_write(item, record_id=";".join(self.component_qids), record_prop='', login=login)
