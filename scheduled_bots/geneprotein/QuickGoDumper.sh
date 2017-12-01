#!/usr/bin/env bash
set -xe

URL="ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/UNIPROT/goa_uniprot_all.gaf.gz"
FILE='goa_uniprot_all.gaf.gz'
FILEOUT='goa_uniprot_all_filt.gaf.gz'


# delete trigger if exists
rm -f -- TRIGGER

# check if download is needed
ls -l
if [ ! -f $FILE ]; then
    echo "File not found!"
    CURRENT_TS="fake news"
else
    CURRENT_TS=`stat -c %y $FILE`
fi

wget -N $URL
NEW_TS=`stat -c %y $FILE`
if [ "$CURRENT_TS" == "$NEW_TS" ]; then
    echo "time stamps match, exiting"
    exit 0
fi

# if we haven't exited, proceed with file parsing

# cut off the header and select only the columns we want
zcat $FILE | grep -v '^!' | awk 'BEGIN{FS="\t"}{print $1,$2,$5,$6,$7,$9,$13,$15}' | gzip > $FILEOUT
#mv tmp2.gz tmp.gz

# to select human for example
#zcat $FILEOUT | awk '$7=="taxon:9606"' > 9606.gaf
#zcat $FILEOUT | awk '$7=="taxon:35758"' > 35758.gaf



touch TRIGGER
ls -l
