import json
import pprint
import time

with open("doid.json", "r") as read_file:
    data = json.load(read_file)

i = 0
for graphs in data["graphs"]:
    if graphs["id"] == 'http://purl.obolibrary.org/obo/doid.owl':
        print('================')
        for node in graphs["nodes"]:
            if node['type'] == 'CLASS':
                i += 1
                try:
                    print('doid', node['id'])
                    print('label', node['lbl'])
                    if 'meta' in node.keys():
                        if "synonyms" in node['meta'].keys():
                            for synonym in node['meta']['synonyms']:
                                print(synonym['pred'], ":", synonym["val"])
                except:
                    pprint.pprint(node)
                    time.sleep(5)
print(i)

