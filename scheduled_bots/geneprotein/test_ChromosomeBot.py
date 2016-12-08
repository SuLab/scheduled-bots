from datetime import datetime
from .ChromosomeBot import get_or_create
from ..geneprotein import organism_info


def test_get_or_create():
    wdid = get_or_create("XII", "NC_001144.5", organism_info[559292], datetime.now())
    assert wdid == 'Q27525657'

