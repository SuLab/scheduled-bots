#!/usr/bin/env bash

if [ -z ${MONGO_URI+x} ]; then echo "MONGO_URI is unset"; exit 1; fi
if [ -z ${INTERPROVERSION+x} ]; then echo "INTERPROVERSION is unset"; exit 1; fi
if [ -z ${INTERPRODATE+x} ]; then echo "INTERPRODATE is unset"; exit 1; fi

TAXA=$(python3 -c "from scheduled_bots.interpro import get_all_taxa; print(','.join(sorted(get_all_taxa())))")

for i in ${TAXA//,/ }; do
    echo "RUNNING $i"
    python3 bot.py --protein --taxon $i --mongo-uri $MONGO_URI --interpro-version $INTERPROVERSION --interpro-date $INTERPRODATE
    echo "FINISHED $i"
done


