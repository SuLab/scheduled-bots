#!/usr/bin/env bash
set -xe

URL1="ftp://ftp.ebi.ac.uk/pub/databases/interpro/current/interpro.xml.gz"
URL2="ftp://ftp.ebi.ac.uk/pub/databases/interpro/current/protein2ipr.dat.gz"
FILE="interpro.xml.gz"
FILEOUT="interpro.json"

# delete trigger if exists
rm -f -- TRIGGER
rm -f -- properties

# check if download is needed
ls -l
if [ ! -f $FILE ]; then
    echo "File not found!"
    CURRENT_TS="fake news"
else
    CURRENT_TS=`stat -c %y $FILE`
fi

wget -N $URL1
wget -N $URL2
NEW_TS=`stat -c %y $FILE`
if [ "$CURRENT_TS" == "$NEW_TS" ]; then
    echo "time stamps match, exiting"
    exit 0
fi
RETRIEVED=$(date -r $FILE +%Y%m%d)
echo "RETRIEVED=$RETRIEVED" > properties

# if we haven't exited, proceed with file parsing
rm -f -- interpro.json
rm -f -- interpro_release.json
rm -f -- interpro_protein.shelve

python3 interpro_parser.py interpro.xml.gz protein2ipr.dat.gz

touch TRIGGER
ls -l
