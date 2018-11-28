"""
Checks to see a file in a github repo is different than the file with the same name on disk
If they are, a file name TRIGGER is created in the current directory
otherwise, TRIGGER is removed

Requirements: depends on 'wget' and 'jq'

"""
import click
import os
import subprocess
from datetime import datetime
from urllib.parse import quote_plus


def get_last_modified(owner, repo, path):
    path = quote_plus(path)
    url = "https://api.github.com/repos/{owner}/{repo}/commits?path={path}".format(owner=owner, repo=repo, path=path)
    last_modified = subprocess.getoutput("curl -s {} | jq -r '.[0].commit.committer.date'".format(url))
    last_modified = datetime.strptime(last_modified, '%Y-%m-%dT%H:%M:%SZ')
    return last_modified


def get_file_last_modified(file_path):
    return datetime.fromtimestamp(os.path.getmtime(file_path))


def set_file_last_modified(file_path, dt: datetime):
    dt = dt.strftime("%C%y%m%d%H%M.%S")
    subprocess.check_call("touch -a -m -t {dt} {file_path}".format(dt=dt, file_path=file_path), shell=True)


@click.command()
@click.option('--owner', help="github repo owner", default="DiseaseOntology")
@click.option('--repo', help="github repo name", default="HumanDiseaseOntology")
@click.option('--path', help="path to file", default="src/ontology/doid.owl")
def main(owner, repo, path):
    url = "https://raw.githubusercontent.com/{owner}/{repo}/master/{path}".format(owner=owner, repo=repo, path=path)
    filename = os.path.split(path)[1]

    if os.path.exists("TRIGGER"):
        os.remove("TRIGGER")
        assert not os.path.exists("TRIGGER")

    curr_last_mod = get_last_modified(owner, repo, path)
    print("latest release: {}".format(curr_last_mod))
    existing_last_mod = None
    if os.path.exists(filename):
        existing_last_mod = get_file_last_modified(filename)
    print("current/local release: {}".format(existing_last_mod))
    if (not existing_last_mod) or (existing_last_mod != curr_last_mod):
        print("downloading new release & triggering job")
        subprocess.check_call(["wget", "--quiet", "-N", url])
        set_file_last_modified(filename, curr_last_mod)
        subprocess.check_call(["touch", "TRIGGER"])
    else:
        print("not running job")


if __name__ == "__main__":
    main()
