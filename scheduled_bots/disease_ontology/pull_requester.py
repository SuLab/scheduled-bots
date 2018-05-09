"""
Given a table of changes (gotten from mesh_changes.py, create the change in the working owl file (doid-edit.owl), using
owltools, then create a new branch and commit it to github. Then create a pull request with a comment containing the
metadata.

"""
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from scheduled_bots.disease_ontology.mesh_changes import main
from scheduled_bots.local import GITHUB_PASS
from tabulate import tabulate

import subprocess
import os

GIT_LOCAL_BASE = "/home/gstupp/projects/HumanDiseaseOntology/src/ontology"
GITHUB_USER = "stuppie"
REMOTE_GIT = "stuppie"  # change to "DiseaseOntology" to use the official DO repo. "stuppie" is my fork (for testing)
# REMOTE_GIT = "DiseaseOntology"

# assumes there exists a file "doid-edit.owl" in the current folder
# in my case, this is a softlink to the file in the cloned git repo

def add_xref(owl_path, doid, ext_id, relation="oboInOwl:hasDbXref"):
    # make sure the skos prefix def is in the owl_file
    if not any(line.strip() == "Prefix(skos:=<http://www.w3.org/2004/02/skos/core#>)" for line in open(owl_path)):
        print("adding skos prefix")
        lines = list(open(owl_path).readlines())
        lines.insert(0, "Prefix(skos:=<http://www.w3.org/2004/02/skos/core#>)\n")
        with open(owl_path, 'w') as f:
            f.writelines(lines)

    # needs owltools in path
    if os.path.exists("tmp.ttl"):
        os.remove("tmp.ttl")
    prefix = """
    @prefix :      <http://purl.obolibrary.org/obo/doid.owl#> .
    @prefix owl:   <http://www.w3.org/2002/07/owl#> .
    @prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
    @prefix oboInOwl: <http://www.geneontology.org/formats/oboInOwl#> .
    @prefix xml:   <http://www.w3.org/XML/1998/namespace> .
    @prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
    @prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
    @prefix obo:   <http://purl.obolibrary.org/obo/> .
    @prefix skos:   <http://www.w3.org/2004/02/skos/core#> .
    """
    ttl = 'obo:DOID_{}  {}  "{}"^^xsd:string .'.format(doid.split(":")[1], relation, ext_id)
    # if relation == "skos:exactMatch":
    #     # to maintain backward compatibility with hasDbXref, add this also if the relation is an exact match
    #     ttl += '\nobo:DOID_{}  {}  "{}"^^xsd:string .'.format(doid.split(":")[1], "oboInOwl:hasDbXref", ext_id)
    with open("tmp.ttl", "w") as f:
        print(prefix, file=f)
        print(ttl, file=f)

    subprocess.check_call("owltools {} --merge tmp.ttl -o -f ofn {}".format(owl_path, owl_path), shell=True)
    lines = [line for line in open(owl_path) if
             'Annotation(rdfs:comment "Includes Ontology(OntologyID(Anonymous' not in line]
    with open(owl_path, 'w') as f:
        f.writelines(lines)


def commit_and_push_changes(branch_id, to_add="doid-edit.owl", msg='add xref'):
    """
    git checkout tmp
    git add doid-edit.owl
    git commit -m "add DOID_0060330 MESH:C535289 xref"
    git push --set-upstream origin tmp
    """
    cd = os.getcwd()
    os.chdir(GIT_LOCAL_BASE)
    subprocess.check_call(["git", "checkout", "-b", branch_id])
    subprocess.check_call(["git", "add", to_add])
    subprocess.check_call(["git", "commit", "-m", "'{}'".format(msg)])
    subprocess.check_call("git push --set-upstream origin {}".format(branch_id), shell=True)
    subprocess.check_call(["git", "checkout", "master"])
    os.chdir(cd)


def create_pullrequest(title, body, branch_id):
    data = {
        "title": title,
        "body": body,
        "head": branch_id,
        "base": "master"
    }
    url = "https://api.github.com/repos/{}/HumanDiseaseOntology/pulls".format(REMOTE_GIT)
    r = requests.post(url, json=data, auth=HTTPBasicAuth(GITHUB_USER, GITHUB_PASS))
    assert r.status_code == 201, r.text
    return r


if __name__ == "__main__":
    df, df_fmt = main("2017-11-28")
    df.to_csv('df_2017-11-28.csv')
    df_fmt.to_csv('df_fmt_2017-11-28.csv')
    # df = pd.read_csv('df_2017-11-28.csv', index_col=0)
    # df_fmt = pd.read_csv('df_fmt_2017-11-28.csv', index_col=0)

    df_fmt = df_fmt.rename(columns={'doid': 'DOID', 'do_label': 'DOID Label',
                                    'do_def': 'DOID Description', 'mesh': 'MeSH ID',
                                    'mesh_label': 'MeSH Label', 'mesh_descr': 'MeSH Description',
                                    'mesh_synonyms': 'MeSH Synonyms',
                                    'qid': 'Wikidata QID', 'wd_label': 'Wikidata Label',
                                    'relation': 'Relation'})

    for idx in range(len(df)):
        # break
        row = df.iloc[idx]
        doid = row.doid
        ext_id = "MESH:" + row.mesh
        # if row.relation == "oboInOwl:hasDbXref":
        #    row.relation = "skos:exactMatch"
        #    df_fmt.iloc[idx:idx + 1].Relation = "skos:exactMatch"
        relation = row.relation
        branch_id = "_".join([doid, ext_id, relation])
        branch_id = branch_id.replace(":", "_")  # can't have : in branch names
        table = df_fmt.iloc[idx:idx + 1].transpose()
        table.columns = ["Value"]

        add_xref("doid-edit.owl", doid, ext_id, relation)
        msg = "add xref: {} {}".format(doid, ext_id)
        commit_and_push_changes(branch_id=branch_id, msg=msg)
        t = tabulate(table, headers='keys', tablefmt='pipe')
        create_pullrequest(title=msg, body=t, branch_id=branch_id)
