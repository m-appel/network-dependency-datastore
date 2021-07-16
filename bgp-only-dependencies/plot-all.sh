#!/bin/bash
set -euo pipefail

if [ ! $# -eq 1 ]
then
    echo "usage: $0 <topic-dump>"
    exit 1
fi

readonly DUMP=$1

python3 plot-cdf.py -f figs/daily/ "$DUMP"

