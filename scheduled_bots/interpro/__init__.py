from wikidataintegrator import wdi_helpers, wdi_core


def get_interpro_releases():
    """
    returns {'58.0': 'Q27877335',
     '59.0': 'Q27135875',
     ...
     }
     """
    return wdi_helpers.id_mapper("P393", (("P629", "Q3047275"),))

INTERPRO_RELEASES = get_interpro_releases()


def remove_deprecated_statements(qid, frc, release_wdid, props, login):
    releases = set(INTERPRO_RELEASES.values()) | {'Q3047275'}
    releases = set(int(x.replace("Q", "")) for x in releases)
    # don't count this release
    releases.discard(int(release_wdid.replace("Q", "")))

    # make sure we have these props in frc
    for prop in props:
        frc.write_required([wdi_core.WDString("fake value", prop)])
    orig_statements = frc.reconstruct_statements(qid)

    s_dep = []
    for s in orig_statements:
        if any(any(x.get_prop_nr() == 'P248' and x.get_value() in releases for x in r) for r in s.get_references()):
            setattr(s, 'remove', '')
            s_dep.append(s)

    if s_dep:
        print("-----")
        print(qid)
        print(orig_statements)
        print(s_dep)
        print([(x.get_prop_nr(), x.value) for x in s_dep])
        print([(x.get_references()[0]) for x in s_dep])
        wd_item = wdi_core.WDItemEngine(wd_item_id=qid, domain='none', data=s_dep, fast_run=False)
        wdi_helpers.try_write(wd_item, '', '', login, edit_summary="remove deprecated statements")


def get_all_taxa():
    # get all taxa with a uniprot protein
    # http://tinyurl.com/hkdwzq9
    query = """SELECT ?t
    {	?a	wdt:P352	?p	; wdt:P703	?t}
    GROUP BY ?t
    """
    result = wdi_core.WDItemEngine.execute_sparql_query(query=query)
    taxa = set([x['t']['value'].replace("http://www.wikidata.org/entity/","")  for x in result['results']['bindings']])
    return taxa

"""
#USe api: http://www.uniprot.org/uniprot/?query=organism%3A%22Homo+sapiens+%28Human%29+%5B9606%5D%22&sort=score
import requests
import pandas as pd
from io import StringIO
params = {'compress': 'no',
          'format': 'tab',
          'force': 'yes',
          'query': 'organism:243230',
          'columns': 'id,entry name,reviewed,protein names,genes,organism,length,database(InterPro)'}
r = requests.get('http://www.uniprot.org/uniprot/', params=params)

df = pd.read_csv(StringIO(r.text), sep='\t')
"""