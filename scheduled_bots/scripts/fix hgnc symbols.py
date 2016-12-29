"""
One off script to remove incorrect hgnc symbol props from human genes
https://bitbucket.org/sulab/wikidatabots/issues/99/human-genes-with-wrong-hgnc-gene-symbols
https://github.com/stuppie/scheduled-bots/issues/2

Example:
# https://www.wikidata.org/w/index.php?title=Q20763635&oldid=376739064
"""
from pymongo import MongoClient
from scheduled_bots.local import WDPASS, WDUSER
from tqdm import tqdm
from wikidataintegrator import wdi_core, wdi_login, wdi_helpers

login = wdi_login.WDLogin(WDUSER, WDPASS)

coll = MongoClient()['wikidata_src']["mygene"]
docs = coll.find(
    {'taxid': 9606, 'type_of_gene': 'protein-coding', 'symbol': {'$exists': True}, 'HGNC': {'$exists': False}})

gene_wdid = wdi_helpers.id_mapper('P351', (('P703', 'Q15978631'),))

for doc in tqdm(docs, total=docs.count()):
    if str(doc['entrezgene']) not in gene_wdid:
        print("{} not in wikidata".format(doc['entrezgene']))
        continue

    wdid = gene_wdid[str(doc['entrezgene'])]
    item = wdi_core.WDItemEngine(wd_item_id=wdid)

    if 'P353' not in item.get_wd_json_representation()['claims']:
        continue
    if len(item.get_wd_json_representation()['claims']['P353']) > 1:
        print("check me: {}".format(item.wd_item_id))

    hgnc_symbol = item.get_wd_json_representation()['claims']['P353'][0]['mainsnak']['datavalue']['value']
    s = wdi_core.WDString(hgnc_symbol, "P353")
    setattr(s, 'remove', '')
    item.update(data=[s])
    item.write(login, edit_summary="remove incorrect HGNC gene symbol")
