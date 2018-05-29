"""
Parses interpro files downloaded from ftp://ftp.ebi.ac.uk/pub/databases/interpro/current/
interpro.xml.gz: interpro family, domain structure
protein2ipr.dat.gz: mapping of individual proteins to family and domains, etc

creates:
interpro_release.json: release info
interpro.json: same as interpro.xml.gz (see parse_interpro_xml)
interpro_protein.shelve: key is uniprot ID, value is {'part_of': list(specific_families), 'has_part': list(has_part)}

"""

import json
import gzip
import shelve
import subprocess
from itertools import groupby

import lxml.etree as et
import sys
from dateutil import parser as dup

from tqdm import tqdm


def parse_release_info(file_path):
    f = gzip.GzipFile(file_path)
    context = iter(et.iterparse(f, events=("start", "end")))
    event, root = next(context)
    d = dict()
    for event, db_item in context:
        if event == "end" and db_item.tag == "dbinfo":
            d[db_item.attrib['dbname']] = dict(db_item.attrib)
        root.clear()
    return d


def parse_interpro_xml(file_path):
    f = gzip.GzipFile(file_path)
    context = iter(et.iterparse(f, events=("start", "end")))
    event, root = next(context)
    d = dict()
    for event, itemxml in context:
        if event == "end" and itemxml.tag == "interpro":
            item = dict(name=itemxml.find('name').text, **itemxml.attrib)
            item['_id'] = item['id']
            item['protein_count'] = int(item['protein_count'])
            parents = [x.attrib['ipr_ref'] for x in itemxml.find("parent_list").getchildren()] if itemxml.find(
                "parent_list") is not None else None
            children = [x.attrib['ipr_ref'] for x in itemxml.find("child_list").getchildren()] if itemxml.find(
                "child_list") is not None else None
            contains = [x.attrib['ipr_ref'] for x in itemxml.find("contains").getchildren()] if itemxml.find(
                "contains") is not None else None
            found_in = [x.attrib['ipr_ref'] for x in itemxml.find("found_in").getchildren()] if itemxml.find(
                "found_in") is not None else None
            if parents:
                assert len(parents) == 1
                item['parent'] = parents[0]
            item['children'] = children
            item['contains'] = contains
            item['found_in'] = found_in
            d[item['_id']] = item
        root.clear()
    return d


def parse_protein_ipr(protein2ipr_path, interpro):
    p = subprocess.Popen(["zcat", protein2ipr_path], stdout=subprocess.PIPE).stdout
    # f = open('protein2ipr.csv.gz', 'w', encoding='utf8')
    # pipe = subprocess.Popen('gzip', stdin=subprocess.PIPE, stdout=f)
    d = shelve.open("interpro_protein.shelve")
    p2ipr = map(lambda x: x.decode('utf-8').strip().split('\t', 2), p)
    for key, lines in tqdm(groupby(p2ipr, key=lambda x: x[0]), total=80362872, miniters=1000000):
        # the total is just for a time estimate. Nothing bad happens if the total is wrong
        ipr_ids = set([x[1] for x in lines])
        # group list of domain in a protein by ipr
        prot_items = [interpro[x] for x in ipr_ids]
        # Of all families, which one is the most precise? (remove families that are parents of any other family in this list)
        families = [x for x in prot_items if x['type'] in {"Family", "Homologous_superfamily"}]
        families_id = set(x['id'] for x in families)
        parents = set(family['parent'] for family in families if 'parent' in family)
        # A protein be in multiple families. ex: http://www.ebi.ac.uk/interpro/protein/A0A0B5J454
        specific_families = families_id - parents
        has_part = [x['id'] for x in prot_items if x['type'] not in {"Family", "Homologous_superfamily"}]
        d[key] = {'part_of': list(specific_families), 'has_part': list(has_part)}
        #s = ",".join([key, "|".join(specific_families), "|".join(has_part)]) + "\n"
        #pipe.stdin.write(s.encode())
    d.close()
    #pipe.communicate()


if __name__ == "__main__":
    # requires paths to interpro.xml.gz and protein2ipr.dat.gz
    interpro_release = parse_release_info(sys.argv[1])
    with open("interpro_release.json", 'w') as f:
        json.dump(interpro_release, f, indent=2)

    interpro = parse_interpro_xml(sys.argv[1])
    with open("interpro.json", 'w') as f:
        json.dump(interpro, f, indent=2)

    parse_protein_ipr(sys.argv[2], interpro)