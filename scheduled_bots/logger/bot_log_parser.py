import glob
import os
import webbrowser
from ast import literal_eval
from functools import lru_cache
from json import JSONDecodeError

import click
import pandas as pd
import json
import sys

import re
import requests
from dateutil.parser import parse as dateutil_parse
from jinja2 import Template

pd.options.display.max_colwidth = 100


@lru_cache()
def get_prop_formatter(pid):
    if not (pid.startswith("P") and isint(pid[1:])):
        return None
    try:
        d = requests.get("https://www.wikidata.org/wiki/Special:EntityData/{}.json".format(pid)).json()
        return d['entities'][pid]['claims']['P1630'][0]['mainsnak']['datavalue']['value']
    except Exception:
        return None


def parse_log(file_path):
    # todo: Actually parse the header and col names
    # note, missing Rev ID in the old logs will just be NaN and won't throw an error
    df = pd.read_csv(file_path, sep=",",
                     names=['Level', 'Timestamp', 'External ID', 'Prop', 'QID', 'Message', 'Msg Type', 'Rev ID'],
                     skiprows=2, dtype={'External ID': str, 'Rev ID': str},
                     comment='#', quotechar='"', skipinitialspace=True, delimiter=';')
    df.fillna('', inplace=True)
    df.replace("None", "", inplace=True)
    df = df.apply(lambda x: x.str.strip())
    df.Timestamp = pd.to_datetime(df.Timestamp, format='%m/%d/%Y %H:%M:%S')
    return df


def gen_ext_id_links(df: pd.DataFrame):
    # given the columns "Prop" and "External ID", if prop is a wikidata property, get the formatter URL
    # and create links to the external ID
    df['External ID'] = df.apply(lambda row: get_ext_id_link(row['Prop'], row['External ID']), axis=1)
    return df


def get_ext_id_link(pid, ext_id):
    formatter = get_prop_formatter(pid)
    if formatter:
        url = formatter.replace("$1", ext_id)
        return '<a href="{}">{}</a>'.format(url, ext_id)
    else:
        return ext_id


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
    try:
        metadata = json.loads(line[1:])
        if 'timestamp' in metadata:
            metadata['timestamp'] = dateutil_parse(metadata['timestamp'])
    except JSONDecodeError as e:
        metadata = {"name": "", "timestamp": "", "run_id": ""}

    df = parse_log(file_path)
    return df, metadata


def generate_summary(df):
    level_counts = df.Level.value_counts().to_dict()
    zlist = list(zip(*[('<a href="#info">Items Processed Succesfully</a>', level_counts.get('INFO', 0)),
                       ('<a href="#warning">Items Skipped Due to a Warning</a>', level_counts.get('WARNING', 0)),
                       ('<a href="#error">Items Skipped Due to an Error</a>', level_counts.get('ERROR', 0))]))
    level_counts = pd.Series(zlist[1], index=zlist[0])
    level_counts.name = "Count"

    info_counts = df.query("Level == 'INFO'").Message.value_counts().to_dict()
    zlist = list(zip(*[('No Action', info_counts.get('SKIP', 0)),
                       ('Update', info_counts.get('UPDATE', 0)),
                       ('Create', info_counts.get('CREATE', 0))]))
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
    # df.is_copy = False
    df.loc[:, col] = df[col].apply(f)
    return df


def escape_html_chars(s):
    return s.replace("&", r'&amp;').replace("<", r'&lt;').replace(">", r'&gt;')


def try_json(s):
    try:
        d = json.loads(s.replace("'", '"'))
        return d
    except Exception:
        return s



def wiki_links_to_html(s):
    # in a string, convert things like "[[Q27826279|Q27826279]]" into
    # '<a href="https://www.wikidata.org/wiki/Q27826279>Q27826279</a>'
    for match in re.findall('\[\[(Q\d{1,12})\|Q\d{1,12}\]\]', s):
        s = s.replace("[[{}|{}]]".format(match, match),
                      '<a href="https://www.wikidata.org/wiki/{}">{}</a>'.format(match, match))
    return s


def format_error(error_type, s):
    # attempts to format an error message, depending on the type of error
    if "WDApiError" in error_type or "NonUniqueLabelDescriptionPairError" in error_type:
        return format_wdapierror(try_json(s))
    if "ManualInterventionReqException" in error_type:
        return format_ManualInterventionReqException(s)
    else:
        return s


def format_ManualInterventionReqException(s):
    # More than one WD item has the same property value Property: P1748, items affected: ['186020', '18557906']
    if "More than one WD item has the same property value" in s:
        pid = s.split("Property: ")[1].split(",")[0].strip()
        qids = literal_eval(s.split("items affected: ")[1])
        urls = ['<a href="https://www.wikidata.org/wiki/{}#{}">{}</a>'.format(x, pid, x) for x in qids]
        return "More than one WD item has the same property value: {}".format(", ".join(urls))
    elif "does not match provided core ID" in s:
        qid = s.split("Retrieved item (")[1].split(")")[0]
        s = s.replace(qid, '<a href="https://www.wikidata.org/wiki/{}">{}</a>'.format(qid, qid))
        return s
    else:
        return s


def format_wdapierror(d):
    # if the message type is a WDApiError, attempt to clean it up a little
    try:
        return {'code': d['error']['code'],
                'info': wiki_links_to_html(d['error']['info']),
                'messages': d['error']['messages']}
    except Exception:
        return d


@click.command()
@click.argument('log-path')
@click.option('--show-browser', default=False, is_flag=True, help='show log in browser')
def main(log_path, show_browser=False):
    if os.path.isdir(log_path):
        # run on all files in dir (that don't end in html), and ignore show_browser
        files = [x for x in glob.glob(os.path.join(log_path, "*")) if
                 not x.endswith(".html") and os.stat(x).st_size != 0]
        for file in files:
            try:
                _main(file)
            except Exception as e:
                print("Parsing log failed: {}".format(file))
    else:
        _main(log_path, show_browser)


def _main(log_path, show_browser=False):
    print(log_path)
    df, metadata = process_log(log_path)
    del df['Timestamp']
    df['Msg Type'] = df['Msg Type'].apply(escape_html_chars)
    df['Message'] = df['Message'].apply(escape_html_chars)
    # df['Message'] = df['Message'].apply(try_json)
    df['Message'] = df.apply(lambda row: format_error(row['Msg Type'], row['Message']), 1)
    df['Rev ID'] = df['Rev ID'].apply(lambda x: '<a href="https://www.wikidata.org/w/index.php?oldid={}&diff=prev">{}</a>'.format(x,x) if x else x)

    level_counts, info_counts, warning_counts, error_counts = generate_summary(df)

    warnings_df = df.query("Level == 'WARNING'")
    warnings_df.is_copy = False
    del warnings_df['Level']
    if not warnings_df.empty:
        warnings_df = gen_ext_id_links(warnings_df)
        warnings_df = url_qid(warnings_df, "QID")

    errors_df = df.query("Level == 'ERROR'")
    errors_df.is_copy = False
    del errors_df['Level']
    if not errors_df.empty:
        errors_df = gen_ext_id_links(errors_df)
        errors_df = url_qid(errors_df, "QID")
        # errors_df['Message'] = errors_df['Message'].apply(try_format_error)

    info_df = df.query("Level == 'INFO'")
    info_df.is_copy = False
    del info_df['Level']
    if not info_df.empty:
        info_df = gen_ext_id_links(info_df)
        info_df = url_qid(info_df, "QID")
        info_df.Message = info_df.Message.str.replace("SKIP", "No Action")

    with pd.option_context('display.max_colwidth', -1):
        # this class nonsense is an ugly hack: https://stackoverflow.com/questions/15079118/js-datatables-from-pandas/41536906
        level_counts = level_counts.to_frame().to_html(escape=False)
        info_counts = info_counts.to_frame().to_html(escape=False)
        warning_counts = warning_counts.to_frame().to_html(escape=False)
        error_counts = error_counts.to_frame().to_html(escape=False)
        info_df = info_df.to_html(escape=False, classes='df" id = "info_df')
        warnings_df = warnings_df.to_html(escape=False, classes='df" id = "warning_df')
        errors_df = errors_df.to_html(escape=False, classes='df" id = "error_df')

    template = Template(open(os.path.join(sys.path[0], "template.html")).read())

    s = template.render(name=metadata['name'], run_id=metadata['run_id'],
                        level_counts=level_counts,
                        info_counts=info_counts,
                        warning_counts=warning_counts,
                        error_counts=error_counts,
                        warnings_df=warnings_df, errors_df=errors_df, info_df=info_df)
    out_path = log_path.rsplit(".", 1)[0] + ".html"
    with open(out_path, 'w') as f:
        f.write(s)

    if show_browser:
        webbrowser.open(out_path)


if __name__ == "__main__":
    main()
