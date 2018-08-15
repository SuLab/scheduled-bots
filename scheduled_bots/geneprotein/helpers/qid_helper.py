from wikidataintegrator import wdi_core

def get_qid_from_refseq(refseq, taxid):
    q = '''SELECT ?protein WHERE {
        ?protein wdt:P637 '{refseq}'.
        ?taxon wdt:P685 '{taxid}'.
        ?protein wdt:P703 ?taxon.
        }
    '''.replace("{refseq}", refseq).replace('{taxid}', str(taxid))
    endpoint = "https://query.wikidata.org/sparql"
    results = wdi_core.WDItemEngine.execute_sparql_query(q, endpoint=endpoint)['results']['bindings']
    if len(results) > 0:
        return results[0]['protein']['value'].split("/")[-1]