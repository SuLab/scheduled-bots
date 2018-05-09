"""
find all variant items
add additional hgvs IDs, and clinvar IDs from clinicalgenome.org

"""
import os
from itertools import chain
from time import gmtime, strftime

import requests

from wikidataintegrator import wdi_core, wdi_login

try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

PROPS = {'ClinVar Variation ID': 'P1929',
         'CIViC variant ID': 'P3329',
         'HGVS nomenclature': 'P3331',
         'stated in': 'P248',
         'retrieved': 'P813',
         'CIViC Variant ID': 'P3329',
         'reference URL': 'P854'
         }

ITEMS = {
    'ClinGen Allele Registry': 'Q43156185'
}

# Note, there are instances in which one HGVS ID has multiple civic IDs
# hgvs_qid = wdi_helpers.id_mapper(PROPS['HGVS nomenclature'], raise_on_duplicate=True)

'''
api docs: http://reg.clinicalgenome.org/doc/AlleleRegistry_0.12.xx_api_v2.pdf
http://reg.clinicalgenome.org/allele?hgvs=NM_004333.4:c.1799T>A
'''

login = wdi_login.WDLogin(WDUSER, WDPASS)
cgurl = "http://reg.clinicalgenome.org/allele"
d = requests.get(cgurl, params={'hgvs': 'NC_000007.13:g.140453136A>T'}).json()

# Get the CAID for all hgvs IDs on a page.
# assert the CAID is the same (or missing, which is fine)
# then get the rest of the info
# if multiple caids, log an error

caid = d['@id'].split("/")[-1]
hgvss = set(chain(*[x['hgvs'] for x in d['genomicAlleles']])) | set(chain(*[x['hgvs'] for x in d['transcriptAlleles']]))
clinvars = [x['variationId'] for x in d['externalRecords']['ClinVarVariations']]

ca_ref_url = "http://reg.clinicalgenome.org/redmine/projects/registry/genboree_registry/by_caid?caid={}"
ref = [wdi_core.WDItemID(ITEMS['ClinGen Allele Registry'], PROPS['stated in'], is_reference=True),
       wdi_core.WDUrl(ca_ref_url.format(caid), PROPS['reference URL'], is_reference=True),
       wdi_core.WDTime(strftime("+%Y-%m-%dT00:00:00Z", gmtime()), PROPS['retrieved'], is_reference=True)]

s = []
for hgvs in hgvss:
    s.append(wdi_core.WDExternalID(hgvs, PROPS['HGVS nomenclature'], references=[ref]))

for clinvar in clinvars:
    s.append(wdi_core.WDExternalID(str(clinvar), PROPS['ClinVar Variation ID'], references=[ref]))

item = wdi_core.WDItemEngine(data=s, wd_item_id="Q21851559")
item.write(login)
