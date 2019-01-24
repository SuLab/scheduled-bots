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


def remove_deprecated_statements(item, release_wdid, props, login):
    releases = set(INTERPRO_RELEASES.values()) | {'Q3047275'}
    releases = set(int(x.replace("Q", "")) for x in releases)
    # don't count this release
    releases.discard(int(release_wdid.replace("Q", "")))

    if item.wd_item_id and item.fast_run and not item.create_new_item:
        # in fastrun mode, make sure we have all statements we need
        frc = item.fast_run_container
        for prop in props:
            frc.write_required([wdi_core.WDString("fake value", prop)])
        orig_statements = frc.reconstruct_statements(item.wd_item_id)
    elif item.wd_item_id and not item.fast_run and not item.create_new_item:
        orig_statements = item.original_statements
    else:
        return None

    s_dep = []
    for s in orig_statements:
        if any(any(x.get_prop_nr() == 'P248' and x.get_value() in releases for x in r) for r in s.get_references()):
            setattr(s, 'remove', '')
            s_dep.append(s)

    if s_dep:
        print("-----")
        print(item.wd_item_id)
        print(orig_statements)
        print(s_dep)
        print([(x.get_prop_nr(), x.value) for x in s_dep])
        print([(x.get_references()[0]) for x in s_dep])
        qid = item.wd_item_id
        wd_item = wdi_core.WDItemEngine(wd_item_id=qid, data=s_dep, fast_run=False)
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