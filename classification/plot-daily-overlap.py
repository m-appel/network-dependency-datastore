import argparse
import bz2
import os
import pickle
import sys
from collections import defaultdict, namedtuple
from datetime import datetime, timedelta

from matplotlib import rcParams
import matplotlib.colors
import matplotlib.pyplot as plt
import msgpack
import numpy as np

from set_permutator import SetPermutator

DATE_FMT = '%Y-%m-%d'
RAW_FILE_TS_FMT = '%Y-%m-%dT00:00'
RAW_FILE_FMT = '{topic}.{timestamp}.pickle.bz2'
OUTPUT_DELIMITER = ','
OUTPUT_EXTENSION = '.csv'
OUTPUT_FILE = '{prefix}.{topic}.{start_ts}--{end_ts}' + OUTPUT_EXTENSION

ScopeDepPair = namedtuple('ScopeDepPair', 'scopes dependencies')
OverlapPair = namedtuple('OverlapPair', 'absolute percentage')


# rcParams['figure.figsize'] = (15, 50)


def parse_timestamp_argument(arg: str) -> datetime:
    return datetime.strptime(arg, DATE_FMT)


def get_raw_file(raw_folder: str, topic: str, timestamp: datetime) -> str:
    file_name = \
        RAW_FILE_FMT.format(topic=topic,
                            timestamp=timestamp.strftime(RAW_FILE_TS_FMT))
    ret = raw_folder + file_name
    if not os.path.exists(ret):
        print(f'Error: File not found: {ret}', file=sys.stderr)
        return str()
    return ret


def process_raw_file(raw_file: str) -> (SetPermutator, SetPermutator):
    se = set()
    sm = set()
    sb = set()
    st = set()
    de = set()
    dm = set()
    db = set()
    dt = set()
    with bz2.open(raw_file, 'rb') as f:
        data = pickle.load(f)
    for msg in data['messages']:
        msg_data = msgpack.loads(msg[1])
        scope = int(msg_data['scope'])
        if len(msg_data['equal']):
            se.add(scope)
        if len(msg_data['mismatched']):
            sm.add(scope)
        if len(msg_data['bgp_only']):
            sb.add(scope)
        if len(msg_data['tr_only']):
            st.add(scope)
        for asn, bgp_score, bgp_rank, tr_score, tr_rank, comp_rank \
                in msg_data['equal']:
            if asn not in de:
                de.add(asn)
        for asn, bgp_score, bgp_rank, bgp_comp_rank, tr_score, tr_rank, \
            tr_comp_rank in msg_data['mismatched']:
            if asn not in dm:
                dm.add(asn)
        for asn, score, rank in msg_data['bgp_only']:
            if asn not in db:
                db.add(asn)
        for asn, score, rank in msg_data['tr_only']:
            if asn not in dt:
                dt.add(asn)
    spermutator = SetPermutator()
    spermutator.add_class('eq', se)
    spermutator.add_class('mm', sm)
    spermutator.add_class('bgp', sb)
    spermutator.add_class('tr', st)
    dpermutator = SetPermutator()
    dpermutator.add_class('eq', de)
    dpermutator.add_class('mm', dm)
    dpermutator.add_class('bgp', db)
    dpermutator.add_class('tr', dt)
    return ScopeDepPair(spermutator, dpermutator)


def calculate_overlap(data: dict) -> (list, dict):
    dates = None
    overlap_per_class = dict()
    for class_name in data:
        prev_set = None
        overlap_abs = list()
        overlap_percentage = list()
        if dates is None:
            dates = list(data[class_name].keys())
            dates.sort()
        for date in dates:
            if prev_set is None:
                prev_set = data[class_name][date]
                continue
            overlap = prev_set.intersection(data[class_name][date])
            overlap_abs.append(len(overlap))
            if len(prev_set) == 0:
                overlap_percentage.append(0)
            else:
                overlap_percentage.append(100 / len(prev_set) * len(overlap))
            prev_set = data[class_name][date]
        overlap_per_class[class_name] = OverlapPair(overlap_abs,
                                                    overlap_percentage)
    return dates, overlap_per_class


def plot_percentage(dates: list, data: dict, output: str) -> None:
    fa = plt.subplots()
    ax: plt.Axes = fa[1]
    for class_name in data:
        vals = data[class_name].percentage
        ax.plot(vals, label=class_name)
    ax.set_ylim(ymin=0)
    ax.set_ylabel('Percentage')
    ax.set_xticks(range(len(dates)))
    ax.set_xticklabels(dates, rotation=45, ha='right')
    ax.legend(loc='lower center', bbox_to_anchor=(0.5, 1), ncol=4)
    # plt.show()
    plt.savefig(output, bbox_inches='tight')


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('raw_folder')
    parser.add_argument('topic')
    parser.add_argument('start_ts')
    parser.add_argument('end_ts')
    parser.add_argument('-d', '--data', default='./')
    parser.add_argument('-f', '--figure', default='./')
    args = parser.parse_args()

    start_ts = parse_timestamp_argument(args.start_ts)
    end_ts = parse_timestamp_argument(args.end_ts)
    raw_folder = args.raw_folder
    if not raw_folder.endswith('/'):
        raw_folder += '/'
    data_folder = args.data
    if not data_folder.endswith('/'):
        data_folder += '/'
    figure_folder = args.figure
    if not figure_folder.endswith('/'):
        figure_folder += '/'

    scope_data = defaultdict(dict)
    dep_data = defaultdict(dict)

    curr_ts = start_ts
    while curr_ts <= end_ts:
        date_key = curr_ts.strftime(DATE_FMT)
        raw_file = get_raw_file(raw_folder, args.topic, curr_ts)
        data = process_raw_file(raw_file)
        for class_name, scopes in data.scopes.get_permutations().items():
            scope_data[class_name][date_key] = scopes
        for class_name, dependencies \
                in data.dependencies.get_permutations().items():
            dep_data[class_name][date_key] = dependencies
        if scope_data.keys() != dep_data.keys():
            print(f'Warning: Classes do not match. Scopes: {scope_data.keys()} '
                  f'Deps: {dep_data.keys()}')
        curr_ts += timedelta(days=1)
    dates, overlap_per_class = calculate_overlap(scope_data)

    data_output = data_folder + \
                  OUTPUT_FILE.format(prefix='scope_overlap_absolute',
                                     topic=args.topic,
                                     start_ts=start_ts.strftime(DATE_FMT),
                                     end_ts=end_ts.strftime(DATE_FMT))
    with open(data_output, 'w') as f:
        f.write(OUTPUT_DELIMITER.join(['class'] + dates) + '\n')
        for class_name, overlaps in overlap_per_class.items():
            f.write(OUTPUT_DELIMITER.join(
                map(str, [class_name] + overlaps.absolute)) + '\n')

    data_output = data_folder + \
                  OUTPUT_FILE.format(prefix='scope_overlap_percentage',
                                     topic=args.topic,
                                     start_ts=start_ts.strftime(DATE_FMT),
                                     end_ts=end_ts.strftime(DATE_FMT))
    with open(data_output, 'w') as f:
        f.write(OUTPUT_DELIMITER.join(['class'] + dates) + '\n')
        for class_name, overlaps in overlap_per_class.items():
            f.write(OUTPUT_DELIMITER.join(
                map(str, [class_name] + overlaps.percentage)) + '\n')

    plot_percentage(dates[1:], overlap_per_class,
                    figure_folder + 'scope-overlap.pdf')

    dates, overlap_per_class = calculate_overlap(dep_data)

    data_output = data_folder + \
                  OUTPUT_FILE.format(prefix='dep_overlap_absolute',
                                     topic=args.topic,
                                     start_ts=start_ts.strftime(DATE_FMT),
                                     end_ts=end_ts.strftime(DATE_FMT))
    with open(data_output, 'w') as f:
        f.write(OUTPUT_DELIMITER.join(['class'] + dates) + '\n')
        for class_name, overlaps in overlap_per_class.items():
            f.write(OUTPUT_DELIMITER.join(
                map(str, [class_name] + overlaps.absolute)) + '\n')

    data_output = data_folder + \
                  OUTPUT_FILE.format(prefix='dep_overlap_percentage',
                                     topic=args.topic,
                                     start_ts=start_ts.strftime(DATE_FMT),
                                     end_ts=end_ts.strftime(DATE_FMT))
    with open(data_output, 'w') as f:
        f.write(OUTPUT_DELIMITER.join(['class'] + dates) + '\n')
        for class_name, overlaps in overlap_per_class.items():
            f.write(OUTPUT_DELIMITER.join(
                map(str, [class_name] + overlaps.percentage)) + '\n')

    plot_percentage(dates[1:], overlap_per_class,
                    figure_folder + 'dep-overlap.pdf')


if __name__ == '__main__':
    main()
    sys.exit(0)
