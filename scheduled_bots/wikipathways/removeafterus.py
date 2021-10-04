from wikidataintegrator import wdi_core
import pprint
item = wdi_core.WDItemEngine(wd_item_id="Q42")
pprint.pprint(item.get_wd_json_representation())