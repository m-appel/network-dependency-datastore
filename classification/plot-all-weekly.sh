#!/bin/bash
set -euo pipefail

if [ ! $# -eq 1 ]
then
    echo "usage: $0 <topic-dump>"
    exit 1
fi

readonly DUMP=$1

python3 plot-scopes.py -f figs/weekly/ -d data/weekly/ "$DUMP"
python3 plot-dependencies.py -f figs/weekly/ -d data/weekly/ "$DUMP"
python3 plot-dependency-scope-relation.py -f figs/weekly/ -d data/weekly/ "$DUMP"

