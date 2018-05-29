#!/usr/bin/env bash

# assume the following files exist
# interpro_release.json, interpro.json
# see interpro_parser.py for details

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

INTERPROVERSION=$(jq -r '.INTERPRO.version' interpro_release.json)
INTERPRODATE=$(jq -r '.INTERPRO.file_date' interpro_release.json)

cd scheduled-bots/scheduled_bots/interpro/

python3 bot.py --items --interpro-version $INTERPROVERSION --interpro-date $INTERPRODATE
python3 DeleteBot.py $INTERPROVERSION
python3 ../logger/bot_log_parser.py logs