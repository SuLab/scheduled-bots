"""
Downloads: https://hpo.jax.org/app/download/annotation


Annotation format docs:
http://hpo-annotation-qc.readthedocs.io/en/latest/annotationFormat.html

Last build: http://compbio.charite.de/jenkins/job/hpo.annotations.2018/lastSuccessfulBuild/artifact/misc_2018/phenotype.hpoa
http://compbio.charite.de/jenkins/job/hpo.annotations.2018/


"""
import pandas as pd
from itertools import chain

from tqdm import tqdm

from scheduled_bots import PROPS
from wikidataintegrator import wdi_core, wdi_helpers, wdi_login
from wikicurie import wikicurie

from wikidataintegrator.ref_handlers import update_retrieved_if_new_multiple_refs
from wikidataintegrator.wdi_helpers.publication import PublicationHelper

from scheduled_bots.local import WDUSER, WDPASS
wd_login = wdi_login.WDLogin(WDUSER, WDPASS)

WIKIBASE = True
if WIKIBASE:
    from wikibase_tools import EntityMaker

    mediawiki_api_url = "http://localhost:7171/w/api.php"
    sparql_endpoint_url = "http://localhost:7272/proxy/wdqs/bigdata/namespace/wdq/sparql"
    username = "testbot"
    password = "password"
    maker = EntityMaker(mediawiki_api_url, sparql_endpoint_url, username, password)
    item_engine = wdi_core.WDItemEngine.wikibase_item_engine_factory(mediawiki_api_url, sparql_endpoint_url)
    login = wdi_login.WDLogin("testbot", "password", mediawiki_api_url=mediawiki_api_url)
else:
    sparql_endpoint_url = 'https://query.wikidata.org/sparql'
    item_engine = wdi_core.WDItemEngine
    login = wd_login


DET_METHOD = {
    'TAS': 'Q23190853',
    'IEA': 'Q23190881',
    'PCS': 'Q55239025',
    'ICE': 'Q23190856'  # im assuming this is "IC"
}

cu = wikicurie.CurieUtil()
h = wdi_helpers.WikibaseHelper(sparql_endpoint_url=sparql_endpoint_url)
DET_METHOD = {k: h.get_qid(v) for k, v in DET_METHOD.items()}

# PROPS = {k: h.get_pid(v) for k, v in PROPS.items()}
for k, v in PROPS.items():
    try:
        PROPS[k] = h.get_pid(v)
    except Exception:
        PROPS[k] = None

HPO_QID = h.get_qid("Q17027854")

EXT_ID_MAP = dict()


def retrieve_qid_from_curie(curie):
    pid, ext_id_value = cu.parse_curie(curie)
    pid = h.get_pid(pid)
    if pid not in EXT_ID_MAP:
        EXT_ID_MAP[pid] = wdi_helpers.id_mapper(pid, endpoint=h.sparql_endpoint_url)
    if ext_id_value not in EXT_ID_MAP[pid]:
        print("Curie not found: {}".format(curie))
        return None
    qid = EXT_ID_MAP[pid][ext_id_value]
    return qid


pd.set_option('display.max_columns', 50)

# disease -> phenotypes
dfdp = pd.read_csv("phenotype.hpoa", sep='\t')

dfdp['disease_curie'] = dfdp['#DB'].map(str) + ":" + dfdp['DB_Object_ID'].map(str)
dfdp = dfdp.query("Aspect == 'P'")
dfdp = dfdp[dfdp.Frequency.isnull() | dfdp.Frequency.str.startswith("HP:")]
dfdp = dfdp[dfdp.Qualifier.isnull()]
dfdp = dfdp[dfdp['#DB'].isin({'OMIM', 'ORPHA'})]

all_refs = set(chain(*dfdp.DB_Reference.str.split(";")))
all_pmids = set(x[5:] for x in all_refs if x.startswith("PMID:"))

# if wikibase, create these pmid items
if WIKIBASE:
    all_pmid_wd_qid = wdi_helpers.get_values("P698", all_pmids)
    # maker.make_entities(sorted(pmid_wd_qid.values()))
    all_pmid_qid = dict()
    for pmid, wd_qid in tqdm(all_pmid_wd_qid.items()):
        qid = wdi_helpers.prop2qid(h.get_pid("P1709"), "http://www.wikidata.org/entity/{}".format(wd_qid),
                                   endpoint=sparql_endpoint_url, value_type='uri')
        if qid:
            all_pmid_qid[pmid] = qid
else:
    for pmid in all_pmids:
        p = PublicationHelper(pmid, 'pmid', 'europepmc').get_or_create(wd_login)
    all_pmid_qid = wdi_helpers.get_values("P698", all_pmids)


for col in {'Qualifier', 'Onset', 'Frequency', 'Sex', 'Modifier', 'Aspect', 'Date_Created', 'Assigned_By',
            'DB_Object_ID', '#DB'}:
    del dfdp[col]

# dfdp = dfdp[dfdp.DB_Reference.str.contains("PMID")]
gb = dfdp.groupby("disease_curie")
gb = list(gb)
for disease_curie, thisdf in tqdm(gb[7113:]):
    disease_qid = retrieve_qid_from_curie(disease_curie)
    if not disease_qid:
        continue
    data = []
    for _, row in thisdf.iterrows():
        det_method = row.Evidence
        hpo_id = row.HPO_ID
        refs = row.DB_Reference.split(";")
        ref_pmids = set(x[5:] for x in refs if x.startswith("PMID:"))
        pmid_qids = set(all_pmid_qid[pmid] for pmid in ref_pmids if pmid in all_pmid_qid)
        hpo_qid = retrieve_qid_from_curie(hpo_id)
        if not hpo_qid:
            continue

        qualifiers = [wdi_core.WDItemID(DET_METHOD[det_method], PROPS['determination method'], is_qualifier=True)]
        ref = [wdi_core.WDItemID(HPO_QID, PROPS['stated in'], is_reference=True)]
        ref.extend([wdi_core.WDItemID(pmid_qid, PROPS['stated in'], is_reference=True) for pmid_qid in pmid_qids])

        s = wdi_core.WDItemID(hpo_qid, PROPS['symptoms'],
                              qualifiers=qualifiers, references=[ref])
        data.append(s)

    item = item_engine(wd_item_id=disease_qid, data=data, append_value=PROPS['symptoms'],
                       global_ref_mode='CUSTOM', ref_handler=update_retrieved_if_new_multiple_refs)
    item.write(login)


"""
# notes/todo:
rows where dfdp[dfdp.Evidence == "PCS"], DB_Reference is a semicolon separated list of pmids (PMID:18382993;PMID:12210303)
Qualifier: is either nan or the string "NOT"
Frequency: can be a HP term or something else. filter out the something elses (low counts anyways)
Aspect: Need to fitler for "P" to get the phenotypes/symptoms
Can look into M and C after, but mean different things

TODO: whhat is Modifier ??

"""
