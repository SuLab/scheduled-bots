from time import mktime
import time
from datetime import datetime
from itertools import chain

import itertools
import json

import pickle

import copy
import pandas as pd
from cachetools import cached, TTLCache
from pymongo.errors import DuplicateKeyError
from tqdm import tqdm

from scheduled_bots.pbb_tracker.connect_mysql import query_wikidata_mysql
from wikidataintegrator.wdi_helpers import id_mapper
from scheduled_bots.local import WDUSER, WDPASS
from pymongo import MongoClient

CACHE_SIZE = 99999
CACHE_TIMEOUT_SEC = 300  # 5 min

from mwclient import Site

site = Site(('https', 'www.wikidata.org'))
site.login(WDUSER, WDPASS)


def chunks(iterable, size):
    it = iter(iterable)
    item = list(itertools.islice(it, size))
    while item:
        yield item
        item = list(itertools.islice(it, size))


@cached(TTLCache(CACHE_SIZE, CACHE_TIMEOUT_SEC))
def getConceptLabels(qids):
    qids = "|".join({qid.replace("wd:", "") if qid.startswith("wd:") else qid for qid in qids})
    wd = site.api('wbgetentities', **{'ids': qids, 'languages': 'en', 'format': 'json', 'props': 'labels'})['entities']
    return {k: v['labels']['en']['value'] if 'labels' in v and 'en' in v['labels'] else '' for k, v in wd.items()}


def isint(x):
    try:
        int(x)
    except Exception:
        return False
    return True


class Change:
    def __init__(self, change_type, qid='', pid='', value='', value_label='', user='', timestamp='', reference=list(),
                 revid=None, comment=''):
        self.change_type = change_type
        self.qid = qid
        self.qid_label = ''
        self.pid = pid
        self.pid_label = ''
        self.value = value
        self.value_label = value_label
        self.user = user
        self.timestamp = timestamp
        self.count = 0
        self.metadata = dict()
        self.reference = reference
        self.ref_list = []
        self.revid = revid
        self.comment=comment

    def __repr__(self):
        return " | ".join(map(str, [self.change_type, self.qid, self.qid_label, self.pid, self.pid_label, self.value,
                                    self.value_label, self.user]))

    def to_dict(self):
        d = copy.deepcopy(self.__dict__)
        d['has_ref'] = bool(d['reference'])
        del d['reference']
        d['ref_str'] = ",".join(
            "{} ({}):{} ({})".format(x['prop_label'], x['prop'], x['value_label'], x['value']) for x in d['ref_list'])
        del d['ref_list']
        d['merge'] = True if 'merge' in d['comment'] else False
        return d

    @staticmethod
    def lookupLabels(changes):
        pids = set(s.pid for s in changes)
        qids = set(s.qid for s in changes)
        values = set(s.value for s in changes if s.value and s.value.startswith("Q") and isint(s.value[1:]))
        ref_qids = set(chain(*[
            [s['value'] for s in change.ref_list if s['value'] and s['value'].startswith("Q") and isint(s['value'][1:])]
            for change in changes]))
        ref_pids = set(chain(*[[s['prop'] for s in change.ref_list] for change in changes]))
        labels = dict()
        x = pids | qids | values | ref_qids | ref_pids
        for chunk in tqdm(chunks(x, 500), total=len(x) / 500):
            l = getConceptLabels(tuple(chunk))
            labels.update(l)

        for c in changes:
            if c.pid and c.pid in labels:
                c.pid_label = labels[c.pid]
            if c.qid and c.qid in labels:
                c.qid_label = labels[c.qid]
            if c.value and c.value in labels:
                c.value_label = labels[c.value]
            for ref in c.ref_list:
                ref['value_label'] = labels.get(ref['value'], '')
                ref['prop_label'] = labels.get(ref['prop'], '')

    def pretty_refs(self):
        """
        refs = [{'hash': '6a25eeddbaf5d49fc4cbb053c46c837c2ae40581',
       'snaks': {'P248': [{'datavalue': {'type': 'wikibase-entityid',
           'value': {'entity-type': 'item',
            'id': 'Q9049250',
            'numeric-id': 9049250}},
          'hash': 'c452e8fc259131192625f0201037bd6577681ccb',
          'property': 'P248',
          'snaktype': 'value'}]},
       'snaks-order': ['P248']}]
        """
        # "stated in (P248): WikiSkripta (Q9049250)|other prop (P1234): '123'"
        ref_list = []
        for ref in self.reference:
            for snak in chain(*ref['snaks'].values()):
                value = get_claim_value(snak)
                prop = snak['property']
                ref_list.append({'value': value, 'prop': prop, 'value_label': '', 'prop_label': ''})
        self.ref_list = ref_list


def get_claim_value(claim):
    mainsnak = claim
    if 'datavalue' not in mainsnak:
        print("no datavalue: {}".format(mainsnak))
        return None
    if mainsnak['datavalue']['type'] in {'wikibase-entityid'}:
        return mainsnak['datavalue']['value']['id']
    elif mainsnak['datavalue']['type'] in {'external-id', 'string'}:
        return mainsnak['datavalue']['value']
    elif mainsnak['datavalue']['type'] in {'quantity'}:
        v = mainsnak['datavalue']['value']
        if 'lowerBound' in v:
            return '^'.join((v['amount'], v['lowerBound'], v['upperBound'], v['unit']))
        else:
            return '^'.join((v['amount'], v['unit']))
    elif mainsnak['datavalue']['type'] in {'monolingualtext'}:
        return mainsnak['datavalue']['value']['text']
    elif mainsnak['datavalue']['type'] in {'globe-coordinate', 'time', 'commonsMedia'}:
        # print(mainsnak)
        return None
    else:
        print(mainsnak)


def detect_label_change(x, y):
    """
    {'en': {'language': 'en',
            'value': 'this is a label'}}
    """
    if x['labels']['en']['value'] != y['labels']['en']['value']:
        return [("CHANGE", x['labels']['en']['value'], y['labels']['en']['value'])]


def detect_aliases_change(x, y):
    pass


def detect_claim_change(claimsx, claimsy):
    s = []
    if len(claimsx) == 0:
        claimsx = dict()
    if len(claimsy) == 0:
        claimsy = dict()
    # props in x but not in y
    props_missing_y = set(claimsx.keys()) - set(claimsy.keys())
    for prop in props_missing_y:
        for claim in claimsx[prop]:
            s.append(Change("REMOVE", pid=prop, value=get_claim_value(claim['mainsnak']),
                            reference=claim.get('references', [])))

    # props in y but not in x
    props_missing_x = set(claimsy.keys()) - set(claimsx.keys())
    for prop in props_missing_x:
        for claim in claimsy[prop]:
            s.append(Change("ADD", pid=prop, value=get_claim_value(claim['mainsnak']),
                            reference=claim.get('references', [])))

    # for props in both, get the values
    props_in_both = set(claimsx.keys()) & set(claimsy.keys())
    for prop in props_in_both:
        values_x = set(get_claim_value(claim['mainsnak']) for claim in claimsx[prop])
        values_y = set(get_claim_value(claim['mainsnak']) for claim in claimsy[prop])
        # values in x but not in y
        missing_y = values_x - values_y
        # values in y but not in x
        missing_x = values_y - values_x
        for m in missing_y:
            s.append(Change("REMOVE", pid=prop, value=m))
        for m in missing_x:
            ref = [x.get('references', []) for x in claimsy[prop] if m == get_claim_value(x['mainsnak'])][0]
            s.append(Change("ADD", pid=prop, value=m, reference=ref))
    return s


"""
def get_revisions(qid, start):
    raise DeprecationWarning("dont use")
    starts = start.strftime('%Y-%m-%dT00:00:00Z')
    # gets up to the last 50 revisions from `start_date` to `end_date`
    # qid = "Q18557952"
    page = site.pages[qid]
    revisions = page.revisions(start=starts, dir='newer', prop='content|ids|timestamp|user|comment')
    return list(revisions)


def get_new_revisions(qid, start, existing_revids):
    raise DeprecationWarning("dont use")
    # two step process, fetch the revision ids, then only get the content for the new ones
    starts = start.strftime('%Y-%m-%dT00:00:00Z')
    page = site.pages[qid]
    revids = set([x['revid'] for x in page.revisions(start=starts, dir='newer')])
    need_revids = revids - set(existing_revids)
    print("{}: {}".format(qid, need_revids))
    revisions = site.revisions(list(need_revids), prop='content|timestamp')
    return list(revisions)
"""


def detect_changes(revisions, qid):
    c = []
    for idx in range(len(revisions) - 1):
        y = revisions[idx]
        x = revisions[idx + 1]
        claimsx = x['claims']
        claimsy = y['claims']
        changes = detect_claim_change(claimsx, claimsy)
        for change in changes:
            change.qid = qid
            change.user = revisions[idx]['user']
            change.timestamp = revisions[idx]['timestamp']
            change.metadata = revisions[0]['metadata'] if 'metadata' in revisions[0] else dict()
            change.revid = revisions[idx]['revid']
            change.comment = revisions[idx]['comment']
        if changes:
            c.append(changes)
    return list(chain(*c))


def process_changes(changes):
    # if a user adds a value to a prop, and then another user removes it, cancel out both revisions
    # example: https://www.wikidata.org/w/index.php?title=Q27869338&action=history
    sorted(changes, key=lambda x: x.timestamp)
    for c in changes:
        for other in changes:
            if (c != other) and (c.qid == other.qid) and (c.pid == other.pid) and (c.value == other.value):
                if c.change_type == "ADD" and other.change_type == "REMOVE":
                    changes = [x for x in changes if x not in {c, other}]
    return changes


def store_revision(coll, rev, metadata):
    if '*' not in rev:
        # todo: handle reverted revisions
        # {'comment': 'Reverted edits by [[Special:Contributions/2600:1017:B02C:BB37:7C27:4C06:B6D4:C066|2600:1017:B02C:BB37:7C27:4C06:B6D4:C066]] ([[User talk:2600:1017:B02C:BB37:7C27:4C06:B6D4:C066|talk]]) to last revision by [[User:205.197.242.154|205.197.242.154]]',
        # 'parentid': 458175156, 'revid': 458238438, 'texthidden': '', 'timestamp': '2017-02-28T18:03:45Z', 'user': 'ValterVB'}
        return None
    d = json.loads(rev['*'])
    del rev['*']
    d.update(rev)
    d['_id'] = d['revid']
    d['metadata'] = metadata if metadata else dict()
    if isinstance(d['timestamp'], time.struct_time):
        d['timestamp'] = datetime.fromtimestamp(mktime(d['timestamp']))
    elif not isinstance(d['timestamp'], str):
        d['timestamp'] = time.strftime(d['timestamp'], '%Y-%m-%dT%H:%M:%SZ')
    try:
        coll.insert_one(d)
    except DuplicateKeyError:
        pass


### get the revisions from the past year for all diseases
def get_revision_ids(coll, qids, weeks=1):
    revisions = set()
    qids_str = '"' + '","'.join(qids) + '"'
    for week in tqdm(range(weeks)):
        query = '''select rev_id, rev_page, rev_timestamp, page_id, page_namespace, page_title, page_touched FROM revision
                       inner join page on revision.rev_page = page.page_id WHERE
                       rev_timestamp > DATE_FORMAT(DATE_SUB(DATE_SUB(NOW(),INTERVAL {week} WEEK), INTERVAL 1 WEEK),'%Y%m%d%H%i%s') AND
                       rev_timestamp < DATE_FORMAT(DATE_SUB(NOW(), INTERVAL {week} WEEK),'%Y%m%d%H%i%s') AND
                       page_content_model = "wikibase-item" AND
                       page.page_title IN({qids});
                '''.format(qids=qids_str, week=week)
        revision_df = query_wikidata_mysql(query)
        print(len(revision_df))
        print(revision_df.head(2))
        print(revision_df.tail(2))
        revisions.update(set(revision_df.rev_id))
        with open("revisions.pkl", "wb") as f:
            pickle.dump(revisions, f)

    # excluding the ones we already have
    have_revisions = set([x['_id'] for x in coll.find({}, {'id': True})])
    print(len(have_revisions))
    need_revisions = revisions - have_revisions
    print(len(need_revisions))
    return need_revisions


def get_revisions(coll, revision_ids, pid, qid_extid_map):
    for chunk in tqdm(chunks(revision_ids, 100), total=len(revision_ids) / 100):
        revs = site.revisions(chunk, prop='ids|timestamp|flags|comment|user|content')
        for rev in revs:
            qid = rev['pagetitle']
            if rev.get('contentmodel', 'fake') != "wikibase-item":
                continue
            store_revision(coll, rev, {pid: qid_extid_map.get(qid, '')})


def process_revisions(coll):
    # process the changes for each qid
    qids = set(x['id'] if 'id' in x else '' for x in coll.find({}, {'id': True}))
    changes = []
    for qid in tqdm(list(qids)[:]):
        revisions = sorted(coll.find({'id': qid}), key=lambda x: x['timestamp'], reverse=True)
        c = detect_changes(revisions, qid)
        c = process_changes(c)
        c = [x for x in c if x.user not in {"ProteinBoxBot"}]
        changes.extend(c)
    return changes


if __name__ == "__main__":
    coll = MongoClient().wikidata.revisions2
    pid = "P699"
    doid_qid = id_mapper(pid)
    qid_doid = {v: k for k, v in doid_qid.items()}
    qids = doid_qid.values()
    # need_revisions = get_revision_ids(coll, qids, weeks=52)
    # get_revisions(coll, need_revisions, pid, qid_doid)
    changes = process_revisions(coll)
    for change in changes:
        change.pretty_refs()
    Change.lookupLabels(changes)
    df = pd.DataFrame([x.to_dict() for x in changes])
    df.to_csv("doid_ref.csv")