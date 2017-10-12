import sys
import os
from pprint import pprint
import requests
from wikidataintegrator import wdi_core, wdi_login, wdi_property_store
from time import gmtime, strftime
import copy

try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

PROPS = {
    'CIViC Variant ID': 'P3329',
    'instance of': 'P31',
}

wdi_property_store.wd_properties['P3329'] = {
        'datatype': 'string',
        'name': 'CIViC Variant ID',
        'domain': ['genes'],
        'core_id': True
    }

__metadata__ = {
    'name': 'CivicBot',
    'maintainer': 'Andra',
    'tags': ['variant'],
    'properties': list(PROPS.values())
}