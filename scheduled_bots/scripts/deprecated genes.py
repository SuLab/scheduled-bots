"""
get a list of deprecated genes
if they are deprecated, they are not in mygene.info anymore, so check mygene for their existance

"""


from wikidataintegrator import wdi_login, wdi_core, wdi_helpers
from pymongo import MongoClient
hg = wdi_helpers.id_mapper("P351", (("P703", "Q15978631"),))
wd = set(hg.keys())

coll = MongoClient().wikidata_src.mygene
doc = coll.find({'_id': {'$in': list(wd)}})
mg = set([str(x['entrezgene']) for x in doc])
dep_entrez = wd-mg
print("Found {} deprecated genes".format(len(dep_entrez)))