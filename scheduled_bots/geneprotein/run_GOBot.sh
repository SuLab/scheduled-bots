#!/usr/bin/env bash

if [ -z ${MONGO_URI+x} ]; then echo "MONGO_URI is unset"; exit 1; fi
if [ -z ${RETRIEVED+x} ]; then echo "RETRIEVED is unset"; exit 1; fi

TAXA=$(python3 -c "from scheduled_bots.geneprotein.GOBot import get_all_taxa; print(get_all_taxa())")

for i in ${TAXA//,/ }; do
    echo "RUNNING $i"
    python3 GOBot.py --fastrun --taxon $i --mongo-uri $MONGO_URI --retrieved $RETRIEVED
    echo "FINISHED $i"
done
