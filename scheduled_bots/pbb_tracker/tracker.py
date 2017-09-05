import math
import os
import click
import time
import datetime
from time import mktime
from itertools import chain, islice
import json
import copy
import pandas as pd
from cachetools import cached, TTLCache
from pymongo.errors import DuplicateKeyError
from tqdm import tqdm
from mwclient import Site
from pymongo import MongoClient

from scheduled_bots.pbb_tracker.connect_mysql import query_wikidata_mysql
from wikidataintegrator.wdi_helpers import id_mapper

try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

CACHE_SIZE = 99999
CACHE_TIMEOUT_SEC = 300  # 5 min

site = Site(('https', 'www.wikidata.org'))
site.login(WDUSER, WDPASS)


def chunks(iterable, size):
    it = iter(iterable)
    item = list(islice(it, size))
    while item:
        yield item
        item = list(islice(it, size))


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
        self.comment = comment
        self.url = "https://www.wikidata.org/w/index.php?diff=prev&oldid={}".format(self.revid)

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
        x = set(y for y in x if y)
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


def detect_changes(revisions, qid):
    c = []
    for idx in range(len(revisions) - 1):
        y = revisions[idx]
        x = revisions[idx + 1]
        claimsx = x['claims']
        claimsy = y['claims']
        changes = detect_claim_change(claimsx, claimsy)
        changes = [x for x in changes if x]
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
    changes = sorted(changes, key=lambda x: x.timestamp)
    for c in changes:
        for other in changes:
            if (c != other) and (c.qid == other.qid) and (c.pid == other.pid) and (c.value == other.value):
                if c.change_type == "ADD" and other.change_type == "REMOVE":
                    changes = [x for x in changes if x not in {c, other}]
    return changes


def process_ld_changes(changes):
    # only show the label changes if the first and last values are different
    changes = sorted(changes, key=lambda x: x.timestamp)
    if changes[0].value != changes[-1].value:
        return [changes[0], changes[-1]]
    else:
        return []


def process_alias_changes(changes):
    # only show the label changes if the first and last values are different
    changes = sorted(changes, key=lambda x: x.timestamp)
    if changes[0].value != changes[-1].value:
        return [changes[0], changes[-1]]
    else:
        return []


def store_revision(coll, rev, metadata):
    if '*' not in rev:
        # this revision was deleted
        return None
    d = json.loads(rev['*'])
    del rev['*']
    d.update(rev)
    d['_id'] = d['revid']
    d['metadata'] = metadata if metadata else dict()
    if isinstance(d['timestamp'], time.struct_time):
        d['timestamp'] = datetime.datetime.fromtimestamp(mktime(d['timestamp']))
    elif not isinstance(d['timestamp'], str):
        d['timestamp'] = time.strftime(d['timestamp'], '%Y-%m-%dT%H:%M:%SZ')
    try:
        coll.insert_one(d)
    except DuplicateKeyError:
        pass


def get_revisions_past_weeks(qids, weeks):
    """
    Get the revision IDs for revisions on `qids` items in the past `weeks` weeks
    :param qids: set of qids
    :param weeks: int
    :return:
    """
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
    return revisions


def get_revision_ids_needed(coll, qids, weeks=1):
    # Get the revision IDs for revisions on `qids` items in the past `weeks` weeks
    # # excluding the ones we already have in `coll`

    revisions = get_revisions_past_weeks(qids, weeks)
    have_revisions = set([x['_id'] for x in coll.find({}, {'id': True})])
    print(len(have_revisions))
    need_revisions = revisions - have_revisions
    print(len(need_revisions))
    return need_revisions


def download_revisions(coll, revision_ids, pid, qid_extid_map):
    for chunk in tqdm(chunks(revision_ids, 100), total=len(revision_ids) / 100):
        revs = site.revisions(chunk, prop='ids|timestamp|flags|comment|user|content')
        for rev in revs:
            qid = rev['pagetitle']
            if rev.get('contentmodel') != "wikibase-item":
                continue
            store_revision(coll, rev, {pid: qid_extid_map.get(qid, '')})


def process_revisions(coll, qids, weeks):
    # process the changes for each qid
    last_updated = datetime.datetime.now() - datetime.timedelta(weeks=weeks)
    changes = []
    for qid in tqdm(list(qids)[:]):
        revisions = sorted(coll.find({'id': qid}), key=lambda x: x['timestamp'], reverse=True)
        revisions = [r for r in revisions if r['timestamp'] > last_updated]
        c = detect_changes(revisions, qid)
        c = process_changes(c)
        changes.extend(c)
    return changes


def process_lda_revisions(coll, qids, weeks):
    # we only care about what happened between the first and last revision
    # not capturing intermediate changes
    last_updated = datetime.datetime.now() - datetime.timedelta(weeks=weeks)
    changes = []
    for qid in tqdm(list(qids)[:]):
        revisions = sorted(coll.find({'id': qid}), key=lambda x: x['timestamp'], reverse=True)
        revisions = [r for r in revisions if r['timestamp'] > last_updated]
        if not revisions:
            continue
        x = revisions[0]
        y = revisions[-1]

        user = x['user']
        timestamp = x['timestamp']
        revid = x['revid']
        comment = x['comment']

        xl = x['labels']['en']['value'] if 'en' in x['labels'] else ''
        yl = y['labels']['en']['value'] if 'en' in y['labels'] else ''
        if xl != yl:
            changes.append(Change(change_type="labels", value=xl, qid=qid,
                                  user=user, timestamp=timestamp, revid=revid, comment=comment))
        xd = x['descriptions']['en']['value'] if 'en' in x['descriptions'] else ''
        yd = y['descriptions']['en']['value'] if 'en' in y['descriptions'] else ''
        if xd != yd:
            changes.append(Change(change_type="descriptions", value=xd, qid=qid,
                                  user=user, timestamp=timestamp, revid=revid, comment=comment))
        x_aliases = set(a['value'] for a in x['aliases']['en']) if 'en' in x['aliases'] else set()
        y_aliases = set(a['value'] for a in y['aliases']['en']) if 'en' in y['aliases'] else set()
        for change in y_aliases - x_aliases:
            changes.append(Change("remove_alias", value=change, qid=qid,
                                  user=user, timestamp=timestamp, revid=revid, comment=comment))
        for change in x_aliases - y_aliases:
            changes.append(Change("add_alias", value=change, qid=qid,
                                  user=user, timestamp=timestamp, revid=revid, comment=comment))
    return changes


@click.command()
@click.option('--pid', default="P699", help='property filter')
@click.option('--idfilter', default='', help='additional filter. example "P703:Q15978631;P1057:Q847102"')
@click.option('--weeks', default=2, help='number of weeks ago')
@click.option('--force_update', default=False, help='skip checking for existing revision')
@click.option('--filter-user', default="ProteinBoxBot", help='filter out changes by this user')
def main(pid, weeks, idfilter, force_update, filter_user):
    """
    from tracker import *
    pid="P699"
    idfilter=""
    weeks=52
    force_update=False
    filter_user="ProteinBoxBot"
    """
    coll_name = pid + "_" + idfilter if idfilter else pid
    save_name = coll_name + "_" + str(datetime.date.today()) + "_{}weeks".format(weeks) + ".xls"
    coll = MongoClient().wikidata[coll_name]
    coll.create_index("id")
    idfilter = [(k.split(":")[0], k.split(":")[1]) for k in idfilter.split(";")] if idfilter else []
    extid_qid = id_mapper(pid, idfilter)
    qid_extid = {v: k for k, v in extid_qid.items()}
    qids = extid_qid.values()

    # what are the date extents of these items?
    # get the most recent timestamp and figure out how many weeks ago it was
    # warning, only checks the most recent timestamp!
    # as in, if you request one week, and then one year, it won't get anything before one week ago
    # unless force_update=True
    weeks_to_dl = weeks
    if not force_update:
        timestamps = set(x['timestamp'] for x in coll.find({'id': {'$in': list(qids)}}, {'timestamp': True}))
        if timestamps:
            if datetime.date.today() == max(timestamps).date():
                print("most recent revision is today, skipping")
                weeks_to_dl = 0
            else:
                weeks_to_dl = math.ceil(abs((max(timestamps) - datetime.datetime.now()).days / 7)) + 1
                print("Most recent revision stored: {}".format(max(timestamps)))
    print("Getting revisions from the past {} weeks".format(weeks_to_dl))

    need_revisions = get_revision_ids_needed(coll, qids, weeks=weeks_to_dl)
    download_revisions(coll, need_revisions, pid, qid_extid)

    print("Processing changes in the past {} weeks".format(weeks))
    changes = process_revisions(coll, qids, weeks)
    for change in changes:
        change.pretty_refs()
    Change.lookupLabels(changes)
    df = pd.DataFrame([x.to_dict() for x in changes])
    # reorder columns
    df = df[["revid", "url", "timestamp", "user", "change_type", "comment", "has_ref", "merge",
             "metadata", "qid", "qid_label", "pid", "pid_label", "value", "value_label", "ref_str"]]
    writer = pd.ExcelWriter(save_name)
    df.to_excel(writer, sheet_name="changes")

    if filter_user:
        df = df.query("user != @filter_user")
    df = df.query("user != 'KrBot'")
    df.to_excel(writer, sheet_name="changes_filtered")

    print("Processing label changes in the past {} weeks".format(weeks))
    lda_changes = process_lda_revisions(coll, qids, weeks)
    Change.lookupLabels(lda_changes)
    lda_df = pd.DataFrame([x.to_dict() for x in lda_changes])
    lda_df = lda_df[
        ["revid", "url", "timestamp", "user", "change_type", "comment", "merge", "qid", "qid_label", "value"]]
    lda_df.to_excel(writer, sheet_name="labels")
    writer.save()


if __name__ == "__main__":
    main()
