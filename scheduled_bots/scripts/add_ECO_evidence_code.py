"""
One off script to Map evidence codes between ECO and GO
https://github.com/evidenceontology/evidenceontology/blob/master/gaf-eco-mapping.txt
"""
import datetime

from wikidataintegrator import wdi_core, wdi_login

from scheduled_bots.local import WDPASS, WDUSER

login = wdi_login.WDLogin(WDUSER, WDPASS)

go_evidence_codes = {'EXP': 'Q23173789', 'IDA': 'Q23174122', 'IPI': 'Q23174389', 'IMP': 'Q23174671', 'IGI': 'Q23174952',
                     'IEP': 'Q23175251', 'ISS': 'Q23175558', 'ISO': 'Q23190637', 'ISA': 'Q23190738', 'ISM': 'Q23190825',
                     'IGC': 'Q23190826', 'IBA': 'Q23190827', 'IBD': 'Q23190833', 'IKR': 'Q23190842', 'IRD': 'Q23190850',
                     'RCA': 'Q23190852', 'TAS': 'Q23190853', 'NAS': 'Q23190854', 'IC': 'Q23190856', 'ND': 'Q23190857',
                     'IEA': 'Q23190881', 'IMR': 'Q23190842'}

eco = {'EXP': 'ECO:0000269', 'IBA': 'ECO:0000318', 'IBD': 'ECO:0000319', 'IC': 'ECO:0000305', 'IDA': 'ECO:0000314',
       'IEA': 'ECO:0000501', 'IEP': 'ECO:0000270', 'IGC': 'ECO:0000317', 'IGI': 'ECO:0000316', 'IKR': 'ECO:0000320',
       'IMP': 'ECO:0000315', 'IMR': 'ECO:0000320', 'IPI': 'ECO:0000353', 'IRD': 'ECO:0000321', 'ISA': 'ECO:0000247',
       'ISM': 'ECO:0000255', 'ISO': 'ECO:0000266', 'ISS': 'ECO:0000250', 'NAS': 'ECO:0000303', 'ND': 'ECO:0000307',
       'RCA': 'ECO:0000245', 'TAS': 'ECO:0000304'}

reference = [wdi_core.WDItemID("Q28445410", "P248", is_reference=True),  # stated in ECO
             wdi_core.WDTime(datetime.datetime.now().strftime('+%Y-%m-%dT00:00:00Z'), 'P813', is_reference=True)]

for evidence_code, wdid in go_evidence_codes.items():
    data = [wdi_core.WDString('http://purl.obolibrary.org/obo/{}'.format(eco[evidence_code].replace(":", "_")), 'P1709',
                              references=[reference])]
    item = wdi_core.WDItemEngine(wd_item_id=wdid, data=data)
    item.write(login, edit_summary="add ECO equivalent class")
