import subprocess

from tqdm import tqdm
from scheduled_bots.local import WDPASS, WDUSER
from wikidataintegrator import wdi_helpers, wdi_core, wdi_login
from scheduled_bots.ontology.obographs_GO import GOGraph

# get the TRUE list of deprecated IDs from GO
subprocess.check_call("wget -N http://purl.obolibrary.org/obo/go.json", shell=True)
g = GOGraph("go.json")
deprecated_curies = set([x.id_curie for x in g.deprecated_nodes])

# get GO -> qid mapping
go_qid = wdi_helpers.id_mapper("P686")
qid_go = {v: k for k, v in go_qid.items()}
assert len(go_qid) == len(qid_go)
# if its not, go here and fix it:
# https://query.wikidata.org/#%23Unique%20value%20constraint%20report%20for%20P686%3A%20report%20listing%20each%20item%0A%0ASELECT%20DISTINCT%20%3Fitem1%20%3Fitem1Label%20%3Fitem2%20%3Fitem2Label%20%3Fvalue%20%0A%7B%0A%09%3Fitem1%20wdt%3AP686%20%3Fvalue%20.%0A%09%3Fitem2%20wdt%3AP686%20%3Fvalue%20.%0A%09FILTER%28%20%3Fitem1%20%21%3D%20%3Fitem2%20%26%26%20STR%28%20%3Fitem1%20%29%20%3C%20STR%28%20%3Fitem2%20%29%20%29%20.%0A%09SERVICE%20wikibase%3Alabel%20%7B%20bd%3AserviceParam%20wikibase%3Alanguage%20%22en%22%20%7D%20.%0A%7D%0ALIMIT%20100

deprecated_qids = set([go_qid[go] for go in deprecated_curies if go in go_qid])

# lets see if there are ones that don't have this ref model ...
query = """
select distinct ?item where {
  ?item p:P686 ?s .
  ?s prov:wasDerivedFrom ?ref .
  ?ref pr:P143 wd:Q22230760 .
  }
"""
items = wdi_core.WDItemEngine.execute_sparql_query(query, as_dataframe=True)['item']
qid_old_ref = set(items.str.replace("http://www.wikidata.org/entity/", ""))

# there's like 200 that don't have this old ref model. skip those for now
deprecated_qids = deprecated_qids & qid_old_ref

# do the deed
login = wdi_login.WDLogin(WDUSER, WDPASS)

for qid in tqdm(deprecated_qids):
    go = qid_go[qid]
    item = wdi_core.WDItemEngine(wd_item_id=qid)
    new_ss = []
    for s in item.statements:
        for ref in s.references:
            if any([(x.get_prop_nr() == "P143") and (x.get_value() == 22230760) for x in ref]):
                # if any ref on this statement has this old ref model, deprecate the statement
                new_s = s.__class__(value=s.get_value(), prop_nr=s.get_prop_nr())
                new_s.set_rank("deprecated")
                new_ss.append(new_s)
                break

    item.update(data=new_ss)
    item.write(login)

### ### ###
### remove old statements off items that are NOT deprecated
### ### ###

query = """
select ?item ?p where {
  FILTER(STRSTARTS(STR(?p), "http://www.wikidata.org/prop/" ))
  ?item ?p ?s .
  ?s prov:wasDerivedFrom ?ref .
  ?ref pr:P143 wd:Q22230760 .
}
"""
items = wdi_core.WDItemEngine.execute_sparql_query(query, as_dataframe=True)['item']
qid_old_ref = set(items.str.replace("http://www.wikidata.org/entity/", ""))

# true list of deprecated ids
deprecated_curies = set([x.id_curie for x in g.deprecated_nodes])
deprecated_qids = set([go_qid[go] for go in deprecated_curies if go in go_qid])
# keep only NOT deprecated qids
qid_old_ref = qid_old_ref - deprecated_qids

# if the statement is not deprecated, remove the old ref, keeping any existing refs
# example if it has two refs: https://www.wikidata.org/wiki/Q42895384
# if the statement is not deprecated, has only one ref, and its the old ref, delete the statement
# exmaple: https://www.wikidata.org/wiki/Q24476895 (instance of biological process)

for qid in tqdm(sorted(list(qid_old_ref))[:5]):

    item = wdi_core.WDItemEngine(wd_item_id=qid)
    new_ss = []
    for s in item.statements:  # type: wdi_core.WDBaseDataType
        if s.get_rank() != "normal":
            continue
        delete_me = False
        for ref in s.references:
            if any([(x.get_prop_nr() == "P143") and (x.get_value() == 22230760) for x in ref]):
                delete_me = True
        if delete_me and len(s.references) == 1:
            setattr(s, "remove", "")
            del s.id
        if delete_me and len(s.references) > 1:
            del s.id
            s.set_references([ref for ref in s.references if
                            not any([(x.get_prop_nr() == "P143") and (x.get_value() == 22230760) for x in ref])])
            print(s.get_value())

    new_item = wdi_core.WDItemEngine(wd_item_id=qid, data=item.statements, global_ref_mode="STRICT_OVERWRITE")
    new_item.write(login)
