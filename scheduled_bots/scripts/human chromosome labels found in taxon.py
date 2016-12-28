"""
One off script to change the labels and add `found in taxon` human to human chromosomes
"""

from wikidataintegrator import wdi_core, wdi_login

chrmap = {'NC_000001.11': 'Q430258',
          'NC_000002.12': 'Q638893',
          'NC_000003.12': 'Q668633',
          'NC_000004.12': 'Q836605',
          'NC_000005.10': 'Q840741',
          'NC_000006.12': 'Q540857',
          'NC_000007.14': 'Q657319',
          'NC_000008.11': 'Q572848',
          'NC_000009.12': 'Q840604',
          'NC_000010.11': 'Q840737',
          'NC_000011.10': 'Q847096',
          'NC_000012.12': 'Q847102',
          'NC_000013.11': 'Q840734',
          'NC_000014.9': 'Q138955',
          'NC_000015.10': 'Q765245',
          'NC_000016.10': 'Q742870',
          'NC_000017.11': 'Q220677',
          'NC_000018.10': 'Q780468',
          'NC_000019.10': 'Q510786',
          'NC_000020.11': 'Q666752',
          'NC_000021.9': 'Q753218',
          'NC_000022.11': 'Q753805', }

chrmap2 = {'NC_000023.11': 'Q61333',  # X
           'NC_000024.10': 'Q202771',  # Y
           'NC_012920.1': 'Q27973632'}  # mt

from scheduled_bots.local import WDPASS, WDUSER

login = wdi_login.WDLogin(WDUSER, WDPASS)

for refseq, wdid in chrmap.items():
    s = [wdi_core.WDItemID("Q15978631", "P703")]
    chr_num = int(refseq.split(".")[0].split("_")[1])
    item = wdi_core.WDItemEngine(wd_item_id=wdid, data=s)
    item.set_label("Homo sapiens chromosome {}".format(chr_num))
    item.write(login)

for refseq, wdid in chrmap2.items():
    s = [wdi_core.WDItemID("Q15978631", "P703")]
    item = wdi_core.WDItemEngine(wd_item_id=wdid, data=s)
    item.write(login)