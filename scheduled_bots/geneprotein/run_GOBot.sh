#!/usr/bin/env bash

if [ -z ${RETRIEVED+x} ]; then echo "RETRIEVED is unset"; exit 1; fi

TAXA=$(python3 -c "from scheduled_bots.geneprotein.GOBot import get_all_taxa; print(get_all_taxa())")

for i in ${TAXA//,/ }; do
    echo "RUNNING $i"
    zcat goa_uniprot_all_filt.gaf.gz | awk '$7=="taxon:$i"' > $i.gaf
    python3 GOBot.py --fastrun --taxon $i --retrieved $RETRIEVED
    echo "FINISHED $i"
done
