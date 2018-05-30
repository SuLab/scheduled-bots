#!/usr/bin/env bash

# assumes the 3 files below exist
# see interpro_parser.py for details

INTERPRO_RELEASE_FILE="/var/lib/jenkins/workspace/InterproTrigger/interpro_release.json"
INTERPRO_ITEMS_FILE="/var/lib/jenkins/workspace/InterproTrigger/interpro.json"
INTERPRO_PROTEIN_FILE="/var/lib/jenkins/workspace/InterproTrigger/protein2ipr.csv.gz"

pwd
echo $WDUSER

virtualenv -p python35 venv
source venv/bin/activate
pip install --upgrade pip

git clone https://github.com/sulab/WikidataIntegrator.git
cd WikidataIntegrator
python3 setup.py install
cd ..

git clone https://github.com/sulab/scheduled-bots.git
cd scheduled-bots
python3 setup.py install
pip install -r requirements.txt
cd ..

cd scheduled-bots/scheduled_bots/interpro/
ln -s $INTERPRO_RELEASE_FILE
ln -s $INTERPRO_ITEMS_FILE
ln -s $INTERPRO_PROTEIN_FILE

INTERPROVERSION=$(jq -r '.INTERPRO.version' interpro_release.json)
INTERPRODATE=$(jq -r '.INTERPRO.file_date' interpro_release.json)

python3 bot.py --items --interpro-version $INTERPROVERSION --interpro-date $INTERPRODATE
python3 DeleteBot.py $INTERPROVERSION
python3 ../logger/bot_log_parser.py logs