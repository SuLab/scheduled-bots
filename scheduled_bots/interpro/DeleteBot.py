import argparse
import os

from scheduled_bots.utils import make_deletion_templates, create_rfd
from wikidataintegrator import wdi_core, wdi_helpers

try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")


def get_deprecated_items(releases):
    # releases is a list of qids who when used as "stated in" we should delete the item

    # this is for the old refs. Should already be deleted
    '''
    query = """SELECT ?item ?itemLabel ?ipr WHERE {
      ?item p:P2926 ?s .
      ?s ps:P2926 ?ipr .
      ?s prov:wasDerivedFrom ?ref .
      ?ref pr:P348 "58.0" .
      #SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
    }"""

    bindings = wdi_core.WDItemEngine.execute_sparql_query(query)['results']['bindings']
    qids = {x['item']['value'].rsplit("/")[-1] for x in bindings}
    '''

    query = """
    SELECT ?item ?itemLabel ?iprurl WHERE {
      ?item p:P2926 ?s .
      ?s ps:P2926 ?ipr .
      ?s prov:wasDerivedFrom ?ref .
      ?ref pr:P248 ?release .
      values ?release **releases_str** .
      BIND(IRI(REPLACE(?ipr, '^(.+)$', ?formatterurl)) AS ?iprurl).
      wd:P2926 wdt:P1630 ?formatterurl .
    }"""
    releases_str = '{' + " ".join(['wd:' + x for x in releases]) + '}'
    query = query.replace("**releases_str**", releases_str)
    print(query)
    bindings = wdi_core.WDItemEngine.execute_sparql_query(query)['results']['bindings']
    qids2 = {x['item']['value'].rsplit("/")[-1] for x in bindings}

    items = qids2

    return items


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("current_release", help="The current release. (e.g.: '64.0')")
    parser.add_argument('--title', help='deletion request title', type=str, default="Delete deprecated Interpro Items")
    parser.add_argument('--reason', help='deletion request reason', type=str,
                        default="These items are deprecated")
    parser.add_argument('--force', help='force run if deleting a large number of genes', action='store_true')
    parser.add_argument('--dummy', help='dont actually create the deletion request', action='store_true')
    args = parser.parse_args()

    current_release = args.current_release

    release_qid = wdi_helpers.id_mapper('P393', (('P629', "Q3047275"),))  # interpro releases
    to_remove = {v for k, v in release_qid.items() if k != current_release}
    print(to_remove)

    qids = get_deprecated_items(to_remove)
    print("|".join(qids))
    print(len(qids))
    if len(qids) > 200 and not args.force:
        raise ValueError(
            "Trying to delete {} items. If you really want to do this, re run with --force".format(len(qids)))
    if len(qids) > 0:
        s = make_deletion_templates(qids, args.title, args.reason)
        if not args.dummy:
            create_rfd(s, WDUSER, WDPASS)
        log_path = "deletion_log.txt"
        with open(log_path, 'w') as f:
            f.write("\n".join(qids))
