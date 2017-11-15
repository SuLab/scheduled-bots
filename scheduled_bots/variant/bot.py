"""
find all variant items
add additional hgvs IDs, civic IDs, clinvar IDs from myvariant

"""
import myvariant
from wikidataintegrator import wdi_core, wdi_helpers, wdi_login


PROPS = {'ClinVar Variation ID': 'P1929',
         'CIViC variant ID': 'P3329',
         'HGVS nomenclature': 'P3331',
         'stated in': 'P248',
         'retrieved': 'P813',
         'CIViC Variant ID': 'P3329',
         }

hgvs_qid = wdi_helpers.id_mapper(PROPS['HGVS nomenclature'], raise_on_duplicate=True)


'''
http://myvariant.info/v1/query?q="NM_004333.4:c.1799T>A"
http://myvariant.info/v1/query?q=clinvar.allele_id:22144&fields=clinvar
'''
mv = myvariant.MyVariantInfo()
vd = mv.getvariant(hgvs)