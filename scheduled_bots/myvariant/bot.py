from wikidataintegrator import wdi_core, wdi_login, wdi_helpers
import os
import pprint
import myvariant
import datetime
import copy

mv = myvariant.MyVariantInfo()


try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

login = wdi_login.WDLogin(WDUSER, WDPASS)
__metadata__ = {
    'name': 'ProteinBoxBot',
    'maintainer': 'Andra',
    'tags': ['variant'],
}

def create_reference(dbsnp_id):
    refStatedIn = wdi_core.WDItemID(value="Q5243761", prop_nr="P248", is_reference=True)
    retrieved = datetime.datetime.now()
    timeStringNow = retrieved.strftime("+%Y-%m-%dT00:00:00Z")
    refRetrieved = wdi_core.WDTime(timeStringNow, prop_nr="P813", is_reference=True)
    refDbSNP = wdi_core.WDString(value=dbsnp_id, prop_nr="P6861", is_reference=True)
    return [refStatedIn, refRetrieved, refDbSNP]


query = """
SELECT * WHERE {
   ?qid wdt:P1929 ?clinvar_variantID .
}
"""
results = wdi_core.WDItemEngine.execute_sparql_query(query=query)

for result in results["results"]["bindings"]:
    data=[]
    print(result["clinvar_variantID"]["value"])
    mv_results = mv.query("clinvar.variant_id:"+str(result["clinvar_variantID"]["value"]), fields="dbsnp")
    pprint.pprint(mv_results)
    print(mv_results["hits"][0]["dbsnp"]["rsid"])
    print(mv_results["hits"][0]["dbsnp"]["dbsnp_build"])
    edition_qualifier = wdi_core.WDString(value=str(mv_results["hits"][0]["dbsnp"]["dbsnp_build"]), prop_nr="P393", is_qualifier=True)
    data.append(wdi_core.WDString(mv_results["hits"][0]["dbsnp"]["rsid"],prop_nr="P6861",qualifiers=[edition_qualifier]))
    #dbsnp_reference = create_reference(mv_results["hits"][0]["dbsnp"]["rsid"])
    #for reference in mv_results["hits"][0]["dbsnp"]["citations"]:
    #    pmid_qid, _, _ = wdi_helpers.PublicationHelper(reference, id_type="pmid",
    #                                                   source="europepmc").get_or_create(login if True else None)
    #    if pmid_qid:
    #        data.append(wdi_core.WDItemID(value=pmid_qid, prop_nr="P1343", references=[copy.deepcopy(dbsnp_reference)]))
    page = wdi_core.WDItemEngine(wd_item_id=result["qid"]["value"].replace("http://www.wikidata.org/entity/", ""),data=data)
    print(page.write(login=login))