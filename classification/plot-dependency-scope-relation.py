import argparse
import bz2
import os
import pickle
import sys
from collections import defaultdict
from datetime import datetime, timezone
from itertools import permutations, zip_longest

import msgpack
import numpy as np
import plotly.graph_objects as go

from set_permutator import SetPermutator

DATE_FMT = '%Y-%m-%dT%H:%M:%S'
INPUT_EXTENSION = '.pickle.bz2'
DATA_OUTPUT_EXTENSION = '.csv'
DATA_OUTPUT_DELIMITER = ','
FIG_OUTPUT_EXTENSION = '.svg'

dep_scope_map = defaultdict(set)
sall = set()
se = set()
sm = set()
sb = set()
st = set()
dall = set()
de = set()
dm = set()
db = set()
dt = set()


def grouper(iterable, n, fillvalue=None):
    """Collect data into fixed-length chunks or blocks"""
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


def get_1_1_connection(src: set, dst: set) -> int:
    return len(src.intersection(dst))


def get_1_n_connections(src: set, dst: list) -> list:
    # Map each dependency to a list of scopes that depend on it. Create
    # a source scope set by repeating this for all dependencies.
    src_scopes = {scope for asn in src for scope in dep_scope_map[asn]}
    ret = list()
    for dst_set in dst:
        ret.append(get_1_1_connection(src_scopes, dst_set))
    return ret


def get_n_n_connections(src: list, dst: list) -> list:
    ret = list()
    for src_set in src:
        ret += get_1_n_connections(src_set, dst)
    return ret


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('topic', help='*.pickle.bz2 dump of topic')
    parser.add_argument('-f', '--fig-output', help='Figure output directory',
                        default='./')
    parser.add_argument('-d', '--data-output', help='Data output directory',
                        default='./')

    args = parser.parse_args()

    fig_output_dir: str = args.fig_output
    if not fig_output_dir.endswith('/'):
        fig_output_dir += '/'

    data_output_dir: str = args.data_output
    if not data_output_dir.endswith('/'):
        data_output_dir += '/'

    if not args.topic.endswith(INPUT_EXTENSION):
        print(f'Error: Expected {INPUT_EXTENSION} input file, but got '
              f'{args.topic}', file=sys.stderr)
        sys.exit(1)

    output_file_prefix = os.path.basename(args.topic)[:-len(INPUT_EXTENSION)]
    data_output_file = data_output_dir + 'dependency-scope-relation.' + \
                       output_file_prefix + DATA_OUTPUT_EXTENSION
    matrix_output_file = data_output_dir + \
                         'dependency-scope-relation-matrix.' + \
                         output_file_prefix + DATA_OUTPUT_EXTENSION
    fig_output_file = fig_output_dir + 'dependency-scope-relation.' + \
                      output_file_prefix + FIG_OUTPUT_EXTENSION

    with bz2.open(args.topic, 'r') as f:
        data = pickle.load(f)
    for msg in data['messages']:
        msg_data = msgpack.loads(msg[1])
        scope = msg_data['scope']
        if scope not in sall:
            sall.add(scope)
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
            dep_scope_map[asn].add(scope)
            if asn not in dall:
                dall.add(asn)
            if asn not in de:
                de.add(asn)
        for asn, bgp_score, bgp_rank, bgp_comp_rank, tr_score, tr_rank, \
            tr_comp_rank in msg_data['mismatched']:
            dep_scope_map[asn].add(scope)
            if asn not in dall:
                dall.add(asn)
            if asn not in dm:
                dm.add(asn)
        for asn, score, rank in msg_data['bgp_only']:
            dep_scope_map[asn].add(scope)
            if asn not in dall:
                dall.add(asn)
            if asn not in db:
                db.add(asn)
        for asn, score, rank in msg_data['tr_only']:
            dep_scope_map[asn].add(scope)
            if asn not in dall:
                dall.add(asn)
            if asn not in dt:
                dt.add(asn)

    spermutator = SetPermutator()
    spermutator.add_class('eq', se)
    spermutator.add_class('mm', sm)
    spermutator.add_class('bgp', sb)
    spermutator.add_class('tr', st)
    sclass_permutations = spermutator.get_permutations()
    # Sanity checks
    for a, b in permutations(sclass_permutations.values(), 2):
        if not a.isdisjoint(b):
            print('Error: Scope sets are not disjoint')
            sys.exit(1)
    all_check = set()
    for n, s in sclass_permutations.items():
        all_check.update(s)
    if sall != all_check:
        print(f'Error: Union of separate sets is missing scopes: '
              f'{sall - all_check}')
        sys.exit(1)

    dpermutator = SetPermutator()
    dpermutator.add_class('eq', de)
    dpermutator.add_class('mm', dm)
    dpermutator.add_class('bgp', db)
    dpermutator.add_class('tr', dt)
    dclass_permutations = dpermutator.get_permutations()
    # Sanity checks
    for a, b in permutations(dclass_permutations.values(), 2):
        if not a.isdisjoint(b):
            print('Error: Dependency sets are not disjoint')
            sys.exit(1)
    all_check = set()
    for n, s in dclass_permutations.items():
        all_check.update(s)
    if dall != all_check:
        print(f'Error: Union of separate sets is missing scopes: '
              f'{dall - all_check}')
        sys.exit(1)

    labels = ['all', 'eq', 'mm', 'bgp', 'tr', 'eq mm', 'eq bgp', 'eq tr',
              'mm bgp', 'mm tr', 'bgp tr', 'eq mm bgp', 'eq mm tr',
              'eq bgp tr', 'mm bgp tr', 'eq mm bgp tr']
    deps = [dclass_permutations[class_name] for class_name in labels[1:]]
    scopes = [sclass_permutations[class_name] for class_name in labels[1:]]
    labels += labels[1:]
    x_vals = [0.0] + [0.5] * len(deps) + [1.0] * len(scopes)
    y_vals = [0.0] + list(np.linspace(0, 1, len(deps))) + \
             list(np.linspace(0, 1, len(scopes)))
    dep_values = list(map(len, deps))
    dep_scope_values = get_n_n_connections(deps, scopes)
    total_values = dep_values + dep_scope_values

    # Connections from 'all' node to each dependency node
    source_ids = [0] * len(deps)
    destination_ids = list(range(1, len(deps) + 1))
    # Connection from each dependency node to all scope nodes
    for src in range(1, len(deps) + 1):
        for dst in range(1, len(scopes) + 1):
            dst += len(deps)
            source_ids.append(src)
            destination_ids.append(dst)
    print(f'       nodes: {len(labels)}')
    print(f'   dep nodes: {len(deps)}')
    print(f' scope nodes: {len(scopes)}')
    print(f'     sources: {len(source_ids)}')
    print(f'destinations: {len(destination_ids)}')
    print(f'    a -> _d*: {len(dep_values)}')
    print(f'  _d* -> _s*: {len(dep_scope_values)}')
    print(f'       total: {len(total_values)}')
    print(f'dependencies: {len(dall)}')
    print(f'      scopes: {len(sall)}')

    mm_fig = go.Figure(data=[go.Sankey(valuesuffix=' scopes',
                                       arrangement='snap',
                                       node={'label': labels,
                                             'x': x_vals,
                                             'y': y_vals,
                                             'pad': 5},
                                       link={'source': source_ids,
                                             'target': destination_ids,
                                             'value': total_values})])
    title = data['name'] + ' ' + \
            datetime \
                .fromtimestamp(data['start_ts'] / 1000, tz=timezone.utc) \
                .strftime(DATE_FMT)
    if data['end_ts'] != data['start_ts']:
        title += ' - ' + \
                 datetime \
                     .fromtimestamp(data['end_ts'] / 1000, tz=timezone.utc) \
                     .strftime(DATE_FMT)
    mm_fig.update_layout(title_text=title)

    os.makedirs(fig_output_dir, exist_ok=True)
    mm_fig.write_image(fig_output_file, width=1000, height=800)

    os.makedirs(data_output_dir, exist_ok=True)
    dclass_sizes = {class_name: len(dep_set)
                    for class_name, dep_set in dclass_permutations.items()}
    total_deps = sum(dclass_sizes.values())
    sclass_sizes = {class_name: len(scope_set)
                    for class_name, scope_set in sclass_permutations.items()}
    total_scopes = sum(sclass_sizes.values())

    with open(data_output_file, 'w') as f:
        f.write(DATA_OUTPUT_DELIMITER.join(['class',
                                            'dependencies',
                                            'dependencies(percentage)',
                                            'scopes',
                                            'scopes(percentage)'])
                + '\n')
        for class_name in dclass_sizes:
            dep_count = dclass_sizes[class_name]
            dep_percentage = 100 / total_deps * dep_count
            scope_count = sclass_sizes[class_name]
            scope_percentage = 100 / total_scopes * scope_count
            f.write(DATA_OUTPUT_DELIMITER.join(map(str, [class_name,
                                                         dep_count,
                                                         dep_percentage,
                                                         scope_count,
                                                         scope_percentage]))
                    + '\n')
        f.write(DATA_OUTPUT_DELIMITER.join(['all',
                                            str(total_deps),
                                            '100.0',
                                            str(total_scopes),
                                            '100.0']) + '\n')

    with open(matrix_output_file, 'w') as f:
        f.write(',' + ','.join(labels[1:len(deps) + 1]) + '\n')
        for idx, group in enumerate(grouper(dep_scope_values, len(scopes))):
            f.write(labels[idx + 1] + ',' + ','.join(map(str, group)) + '\n')
    sys.exit(0)
