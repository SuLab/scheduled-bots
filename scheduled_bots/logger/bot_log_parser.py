import glob
import os
import webbrowser

import click
import pandas as pd
import json
import sys
from dateutil.parser import parse as dateutil_parse
from jinja2 import Template

pd.options.display.max_colwidth = 100


def parse_log(file_path):
    df = pd.read_csv(file_path, sep=",",
                     names=['Level', 'Timestamp', 'External ID', 'Prop', 'QID', 'Message', 'Msg Type'],
                     dtype={'External ID': str}, comment='#', quotechar='"', skipinitialspace=True, delimiter=';')
    df.fillna('', inplace=True)
    df = df.apply(lambda x: x.str.strip())
    df.Timestamp = pd.to_datetime(df.Timestamp, format='%m/%d/%Y %H:%M:%S')
    return df


def process_log(file_path):
    """
    Expects header as first line in log file. Header begins with comment character '#'. The line is a json string dump of
    a dictionary that contains the following keys:
    name: str, Task name
    maintainer: str, Name of person
    tags: list of tags associated with the task. can be empty
    properties: list of properties associated with the task. can be empty
    run_id: str, a run ID for the task run
    timestamp: str, timestamp for the task run

    :param file_path:
    :return:
    """
    # read header
    if isinstance(file_path, str):
        with open(file_path) as f:
            line = f.readline()
    else:
        line = file_path.readline()
    if not line.startswith("#"):
        raise ValueError("Expecting header in log file")
    metadata = json.loads(line[1:])
    if 'timestamp' in metadata:
        metadata['timestamp'] = dateutil_parse(metadata['timestamp'])

    df = parse_log(file_path)
    return df, metadata


def generate_summary(df):
    level_counts = df.Level.value_counts().to_dict()
    zlist = list(zip(*[('Items Processed Succesfully', level_counts.get('INFO', 0)),
                       ('Items Skipped Due to Missing Annotations ("Warning")', level_counts.get('WARNING', 0)),
                       ('Items Skipped Due to an Error', level_counts.get('ERROR', 0))]))
    level_counts = pd.Series(zlist[1], index=zlist[0])
    level_counts.name = "Count"

    info_counts = df.query("Level == 'INFO'").Message.value_counts().to_dict()
    zlist = list(zip(*[('No Action', info_counts.get('SKIP', 0)),
                       ('Updated', info_counts.get('UPDATE', 0)),
                       ('Created', info_counts.get('CREATE', 0))]))
    info_counts = pd.Series(zlist[1], index=zlist[0])
    info_counts.name = "Count"

    warning_counts = df.query("Level == 'WARNING'")['Msg Type'].value_counts()
    warning_counts.name = "Count"
    error_counts = df.query("Level == 'ERROR'")['Msg Type'].value_counts()
    error_counts.name = "Count"
    return level_counts, info_counts, warning_counts, error_counts


def isint(s):
    try:
        int(s)
    except Exception:
        return False
    return True


def url_qid(df, col):
    href = "<a href=https://www.wikidata.org/wiki/{}>{}</a>"
    f = lambda x: href.format(x, x) if x.startswith("Q") and isint(x[1:]) else x
    df.is_copy = False
    df.loc[:, col] = df[col].apply(f)
    return df

@click.command()
@click.argument('log-path')
@click.option('--show-browser', default=False, is_flag=True, help='show log in browser')
def main(log_path, show_browser=False):
    if os.path.isdir(log_path):
        # run on all files in dir (that don't end in html), and ignore show_browser
        files = [x for x in glob.glob(os.path.join(log_path, "*")) if not x.endswith(".html")]
        print(files)
        for file in files:
            _main(file)
    else:
        _main(log_path, show_browser)


def _main(log_path, show_browser=False):
    print(log_path)
    df, metadata = process_log(log_path)
    level_counts, info_counts, warning_counts, error_counts = generate_summary(df)
    warnings_df = df.query("Level == 'WARNING'")
    errors_df = df.query("Level == 'ERROR'")
    info_df = df.query("Level == 'INFO'")
    info_df = url_qid(info_df, "QID")

    template = Template(open(os.path.join(sys.path[0], "template.html")).read())
    s = template.render(name=metadata['name'], run_id=metadata['run_id'],
                        level_counts=level_counts.to_frame().to_html(),
                        info_counts=info_counts.to_frame().to_html(),
                        warning_counts=warning_counts.to_frame().to_html(),
                        error_counts=error_counts.to_frame().to_html(),
                        warnings_df=warnings_df.to_html(), errors_df=errors_df.to_html(),
                        info_df=info_df.to_html(escape=False))
    out_path = log_path.rsplit(".", 1)[0] + ".html"
    with open(out_path, 'w') as f:
        f.write(s)

    if show_browser:
        webbrowser.open(out_path)

if __name__ == "__main__":
    main()


