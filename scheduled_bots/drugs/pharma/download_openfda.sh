#!/usr/bin/env bash
set -e
python3 -c "from dumper_openfda import Dumper; d=Dumper(); d.download_files()"

for i in *.zip; do unzip ${i}; done

# extract results, reformat one line per json
cat drug-label-000?-of-*.json | jq -s '.[].results[]' | jq -c '.' | gzip > results.json.gz
cat drug-label-0001-of-*.json | jq -s '.[].meta' > meta.json

rm drug-label-000?-of-*.json*

# zcat results.json.gz | jq -c --arg keys '["openfda", "indications_and_usage", "set_id"]' 'with_entries(select(.key == ($keys | fromjson[])))' > indications.json

zcat results.json.gz | jq -c '.openfda' | gzip > openfda.json.gz
