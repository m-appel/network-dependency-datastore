#!/bin/bash
set -euo pipefail
if [ ! $# -eq 1 ]
then
    echo "usage: $0 <path/to/results>"
    exit 1
fi
DIR=${1%/}

for DUMP in "${DIR}"/*/ihr_hegemony_*.pickle.bz2
do
    python3 extract-data.py "${DUMP}"
done
