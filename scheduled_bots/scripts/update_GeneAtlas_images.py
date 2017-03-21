"""
One off script to change GeneAtlas images to point to full-sized versions
https://github.com/SuLab/GeneWikiCentral/issues/1

As described at https://www.wikidata.org/wiki/Property_talk:P692#How_about_using_full_size_image_instead_of_small_thumbnail.3F
update all uses of the Gene Atlas Image property to use the full-sized version of the Gene Atlas image
(e.g., https://www.wikidata.org/wiki/File:PBB_GE_ACTN3_206891_at_fs.png) instead of the thumbnail
(e.g., https://www.wikidata.org/wiki/File:PBB_GE_ACTN3_206891_at_tn.png)

SELECT ?item ?image
WHERE
{
  ?item wdt:P351 ?entrez .
  ?item wdt:P703 wd:Q15978631 .
  ?item wdt:P692 ?image
} limit 1000
"""
from collections import defaultdict

from scheduled_bots.local import WDPASS, WDUSER
from tqdm import tqdm
from wikidataintegrator import wdi_core, wdi_login, wdi_helpers
import urllib.request

login = wdi_login.WDLogin(WDUSER, WDPASS)

image_qid = wdi_helpers.id_mapper("P692", [("P703", "Q15978631")])
qid_images = defaultdict(list)
for image, qid in image_qid.items():
    qid_images[qid].append(image)
qid_images = dict(qid_images)

for qid, images in tqdm(qid_images.items()):
    images = [urllib.request.unquote(image.replace("http://commons.wikimedia.org/wiki/Special:FilePath/", "")) for image in images]
    images_proc = [image for image in images if image.startswith("PBB GE") and image.endswith("at tn.png")]
    if not images_proc:
        continue
    images_keep = [image for image in images if image.startswith("PBB GE") and image.endswith("at fs.png")]

    item = wdi_core.WDItemEngine(wd_item_id=qid)

    s = []
    for image in images_proc:
        s.append(wdi_core.WDCommonsMedia(image.replace(" at tn.png", " at fs.png"), "P692"))
    for image in images_keep:
        s.append(wdi_core.WDCommonsMedia(image, "P692"))
    item.update(data=s)
    wdi_helpers.try_write(item, '', '', login, edit_summary="replace thumbnail gene atlas image with fs")
