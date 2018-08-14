from wikidataintegrator import wdi_core, wdi_helpers

DAYS = 120
#update_retrieved_if_new = partial(update_retrieved_if_new, days=DAYS)

def remove_deprecated_statements(qid, frc, releases, last_updated, props, login):
    """
    :param qid: qid of item
    :param frc: a fastrun container
    :param releases: list of releases to remove (a statement that has a reference that is stated in one of these
            releases will be removed)
    :param last_updated: looks like {'Q20641742': datetime.date(2017,5,6)}. a statement that has a reference that is
            stated in Q20641742 (entrez) and was retrieved more than DAYS before 2017-5-6 will be removed
    :param props: look at these props
    :param login:
    :return:
    """
    for prop in props:
        frc.write_required([wdi_core.WDString("fake value", prop)])
    orig_statements = frc.reconstruct_statements(qid)
    releases = set(int(r[1:]) for r in releases)

    s_dep = []
    for s in orig_statements:
        if any(any(x.get_prop_nr() == 'P248' and x.get_value() in releases for x in r) for r in s.get_references()):
            setattr(s, 'remove', '')
            s_dep.append(s)
        else:
            for r in s.get_references():
                dbs = [x.get_value() for x in r if x.get_value() in last_updated]
                if dbs:
                    db = dbs[0]
                    if any(x.get_prop_nr() == 'P813' and last_updated[db] - x.get_value() > DAYS for x in r):
                        setattr(s, 'remove', '')
                        s_dep.append(s)
    if s_dep:
        print("-----")
        print(qid)
        print(len(s_dep))
        print([(x.get_prop_nr(), x.value) for x in s_dep])
        print([(x.get_references()[0]) for x in s_dep])
        wd_item = wdi_core.WDItemEngine(wd_item_id=qid, domain='none', data=s_dep, fast_run=False)
        wdi_helpers.try_write(wd_item, '', '', login, edit_summary="remove deprecated statements")