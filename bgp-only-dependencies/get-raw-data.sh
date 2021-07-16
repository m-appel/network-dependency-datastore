#!/bin/bash
set -euo pipefail
if [ ! $# -eq 1 ]
then
	echo "usage: $0 <timestamp>"
	exit 1
fi

readonly DUMP_SCRIPT=path/to/dump-topic.py
readonly TOPIC=ihr_hegemony_classification_bgp_only_dependencies
readonly TS=$1

python3 $DUMP_SCRIPT -o raw/daily/ -ts "$TS" $TOPIC

