#!/usr/bin/env bash

TAXA=$(python3 -c "from scheduled_bots.interpro import get_all_taxa; print(','.join(sorted(get_all_taxa())))")

for i in ${TAXA//,/ }; do
    echo "RUNNING $i"
    python3 bot.py --protein --taxon $i --mongo-uri $MONGO_URI --interpro-version $INTERPROVERSION --interpro-date $INTERPRODATE
    echo "FINISHED $i"
    exit
done


