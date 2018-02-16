# takes the output of 1-filter-normalization (openfda_filtered.json)
import json
import os
from collections import defaultdict
from itertools import chain

from scheduled_bots.drugs.pharma.Product import Product
from tqdm import tqdm

from scheduled_bots.drugs.pharma import rxnorm
from scheduled_bots.drugs.pharma.Mixtures import Mixtures
from wikidataintegrator import wdi_helpers

m = Mixtures()

rxnorm_qid = wdi_helpers.id_mapper("P3345", return_as_set=True)
rxnorm_qid = {k: list(v)[0] for k, v in rxnorm_qid.items() if len(v) == 1}

with open("openfda_filtered.json") as f:
    ds = json.load(f)

# lookup the brandname and ingredients from the rxnorm IDs
rxcuis = set(chain(*[x['rxcui'] for x in ds]))
rxcui_related = json.load(open("rxcui_related.json")) if os.path.exists("rxcui_related.json") else dict()
for rxcui in tqdm(rxcuis):
    if rxcui not in rxcui_related:
        rxcui_related[rxcui] = rxnorm.get_rxcui_related(rxcui)
with open("rxcui_related.json", 'w') as f:
    json.dump(rxcui_related, f)

## from here on, we are using rxnorm only, in order to set up pharmaceutical products and active ingredients
# ignoring what openfda says

# create a mapping between all rxcui's in all related links, as a lookup table of sorts..
rxcui_related_rxcui = dict()
for _, related in rxcui_related.items():
    for vv in chain(*related.values()):
        rxcui_related_rxcui[vv['rxcui']] = {k: [x['rxcui'] for x in v] for k, v in related.items()}

# create a rxcui -> label mapping
rxcui_label = dict()
for _, related in rxcui_related.items():
    for vv in chain(*related.values()):
        rxcui_label[vv['rxcui']] = vv['name']

"""
this is a little confusing, Epzicom (http://bioportal.bioontology.org/ontologies/RXNORM?p=classes&conceptid=497184)
has "abacavir / Lamivudine" (614534) as the ingredients, but it also has the precise ingredient: "abacavir sulfate" (221052)
and the mixture "abacavir / Lamivudine" has as an ingredient "abacavir", not "abacavir sulfate".
Going to try to keep it consistent and use the mixture. May need a new property to add "abacavir sulfate".
"""

# simple example: http://purl.bioontology.org/ontology/RXNORM/202363

# non mixtures
# brand -> ingredient
brand_in = defaultdict(set)
for r in rxcui_related_rxcui.values():
    if r['MIN']:
        continue
    for bn in r['BN']:
        brand_in[bn].add(frozenset(r['IN']))
print(len(brand_in))
# toss those with multiple sets of ingredients (13 of them have conflicting ingredients)
brand_in = {k: set(list(v)[0]) for k, v in brand_in.items() if len(v) == 1}
print(len(brand_in))
# these should not be mixtures, so they should have one ingredient!! (and all do, yay)
brand_in = {k: list(v)[0] for k, v in brand_in.items() if len(v) == 1}
print(len(brand_in))

# mixtures
# brand -> MIN (mixture ingredient)
brand_min = defaultdict(set)
for r in rxcui_related_rxcui.values():
    if r['MIN']:
        for bn in r['BN']:
            brand_min[bn].add(frozenset(r['MIN']))
print(len(brand_min))
# toss those with multiple sets of ingredients (0 of them have conflicting ingredients)
brand_min = {k: set(list(v)[0]) for k, v in brand_min.items() if len(v) == 1}
print(len(brand_min))
# should only have one (they all do, yay)
brand_min = {k: list(v)[0] for k, v in brand_min.items() if len(v) == 1}
print(len(brand_min))

# mixture -> ingredients
min_in = defaultdict(set)
for min in brand_min.values():
    min_in[min].add(frozenset(rxcui_related_rxcui[min]['IN']))
print(len(min_in))
# toss those with multiple conflicting sets of ingredients (0 of them have conflicting ingredients)
min_in = {k: set(list(v)[0]) for k, v in min_in.items() if len(v) == 1}
print(len(min_in))

brand_in_min = brand_in.copy()
brand_in_min.update(brand_min)
################
## actually do stuff here
################

# get or create all mixtures
# min_rxcui, ingredients = next(iter(min_in.items()))
for min_rxcui, ingredients in min_in.items():
    if not all(x in rxnorm_qid for x in ingredients):
        for x in ingredients:
            print("missing ingredients qids: {}, {}".format(x, rxcui_label[x]))
        continue
    qid = m.get_mixture_qid([rxnorm_qid[x] for x in ingredients])
    m.get_or_create(rxcui_label[min_rxcui], min_rxcui, [rxnorm_qid[x] for x in ingredients])

# get or create brandnames/products
for brand, ingredient in tqdm(brand_in_min.items()):
    if brand not in rxnorm_qid and (ingredient in rxnorm_qid):
        p = Product(rxcui=brand, label=rxcui_label[brand])
        qid = p.get_or_create()
        rxnorm_qid[brand] = qid
        ingredient_qid = rxnorm_qid[ingredient]
        p.add_active_ingredient(ingredient_qid)

# create all brandname -> active ingredient links
for brand, ingredient in tqdm(brand_in.items()):
    if (brand not in rxnorm_qid):
        print("missing brand: {} {}".format(brand, rxcui_label[brand]))
    if (ingredient not in rxnorm_qid):
        print("missing ingredient: {} {}".format(ingredient, rxcui_label[ingredient]))
    if (brand not in rxnorm_qid) or (ingredient not in rxnorm_qid):
        continue

    brand_qid = rxnorm_qid[brand]
    ingredient_qid = rxnorm_qid[ingredient]
    p = Product(qid=brand_qid)
    p.add_active_ingredient(ingredient_qid)


"""
query to get everything
https://query.wikidata.org/#select%20%3Fprod%20%3FprodLabel%20%3FingredLabel%20%3Fprod_rxnorm%20%3Fprod_emea%20%3Fingred_rxnorm%20%3Fingred_unii%20%28GROUP_CONCAT%28%3Fpart_unii%3Bseparator%3D%22%2C%22%29%20as%20%3Fp%29%20where%20%7B%0A%20%20%3Fprod%20wdt%3AP31%20wd%3AQ28885102%20.%0A%20%20%3Fprod%20wdt%3AP3781%20%3Fingred%20.%0A%20%20optional%20%7B%3Fprod%20wdt%3AP3345%20%3Fprod_rxnorm%7D%0A%20%20optional%20%7B%3Fprod%20wdt%3AP3637%20%3Fprod_emea%7D%0A%20%20optional%20%7B%3Fingred%20wdt%3AP3345%20%3Fingred_rxnorm%7D%0A%20%20optional%20%7B%3Fingred%20wdt%3AP652%20%3Fingred_unii%7D%0A%20%20optional%20%7B%3Fingred%20wdt%3AP527%20%3Fparts%20.%0A%20%20%20%20%20%20%20%20%20%20%20%20%3Fingred%20wdt%3AP31%20wd%3AQ169336%20.%0A%20%20%20%20%20%20%20%20%20%20%20%20%3Fparts%20wdt%3AP652%20%3Fpart_unii%20.%0A%20%20%20%20%20%20%20%20%20%20%20%20%3Fparts%20rdfs%3Alabel%20%3FpL.%0A%20%20%20%20%20%20%20%20%20%20%20FILTER%28LANG%28%3FpL%29%20%3D%20%22en%22%29.%7D%0A%20%20SERVICE%20wikibase%3Alabel%20%7B%20bd%3AserviceParam%20wikibase%3Alanguage%20%22%5BAUTO_LANGUAGE%5D%2Cen%22.%20%7D%0A%7D%20group%20by%20%3Fprod%20%3FprodLabel%20%3FingredLabel%20%3Fprod_rxnorm%20%3Fprod_emea%20%3Fingred_rxnorm%20%3Fingred_unii%0AORDER%20by%20%3FingredLabel
"""