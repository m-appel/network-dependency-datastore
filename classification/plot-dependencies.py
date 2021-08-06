import argparse
import bz2
import os
import pickle
import sys
from datetime import datetime, timezone
from itertools import permutations

import msgpack
import plotly.graph_objects as go

from set_permutator import SetPermutator

DATE_FMT = '%Y-%m-%dT%H:%M:%S'
INPUT_EXTENSION = '.pickle.bz2'
DATA_OUTPUT_EXTENSION = '.csv'
DATA_OUTPUT_DELIMITER = ','
FIG_OUTPUT_EXTENSION = '.svg'

dall = set()
de = set()
dm = set()
db = set()
dt = set()


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
    data_output_file = data_output_dir + 'dependencies.' + \
                       output_file_prefix + DATA_OUTPUT_EXTENSION
    fig_output_file = fig_output_dir + 'dependencies.' + \
                      output_file_prefix + FIG_OUTPUT_EXTENSION

    with bz2.open(args.topic, 'r') as f:
        data = pickle.load(f)
    for msg in data['messages']:
        msg_data = msgpack.loads(msg[1])
        for asn, bgp_score, bgp_rank, tr_score, tr_rank, comp_rank \
                in msg_data['equal']:
            if asn not in dall:
                dall.add(asn)
            if asn not in de:
                de.add(asn)
        for asn, bgp_score, bgp_rank, bgp_comp_rank, tr_score, tr_rank, \
            tr_comp_rank in msg_data['mismatched']:
            if asn not in dall:
                dall.add(asn)
            if asn not in dm:
                dm.add(asn)
        for asn, score, rank in msg_data['bgp_only']:
            if asn not in dall:
                dall.add(asn)
            if asn not in db:
                db.add(asn)
        for asn, score, rank in msg_data['tr_only']:
            if asn not in dall:
                dall.add(asn)
            if asn not in dt:
                dt.add(asn)
    permutator = SetPermutator()
    permutator.add_class('eq', de)
    permutator.add_class('mm', dm)
    permutator.add_class('bgp', db)
    permutator.add_class('tr', dt)
    class_permutations = permutator.get_permutations()
    # Sanity checks
    for a, b in permutations(class_permutations.values(), 2):
        if not a.isdisjoint(b):
            print('Error: Dependency sets are not disjoint')
            sys.exit(1)
    all_check = set()
    for n, s in class_permutations.items():
        all_check.update(s)
    if dall != all_check:
        print(f'Error: Union of separate sets is missing scopes: '
              f'{dall - all_check}')
        sys.exit(1)

    mixed_mm = set()
    mixed_no_mm = set()
    for class_name in class_permutations:
        classes = len(class_name.split())
        if classes > 1 and 'mm' in class_name:
            mixed_mm.update(class_permutations[class_name])
        elif classes > 1:
            mixed_no_mm.update(class_permutations[class_name])
    mm_fig_classes = class_permutations.copy()
    mm_fig_classes['mixed mm'] = mixed_mm
    mm_fig_classes['mixed no mm'] = mixed_no_mm

    mm_fig_nodes = [('all', 0, 0.1),
                    ('mixed mm', 0.5, 0.1),
                    ('mixed no mm', 0.5, 0.2),
                    ('eq', 0.5, 0.5),
                    ('mm', 0.5, 0.5),
                    ('bgp', 0.5, 0.5),
                    ('tr', 0.5, 0.5),
                    ('eq mm', 1, 0.1),
                    ('mm bgp', 1, 0.1),
                    ('mm tr', 1, 0.1),
                    ('eq mm bgp', 1, 0.2),
                    ('eq mm tr', 1, 0.2),
                    ('mm bgp tr', 1, 0.2),
                    ('eq mm bgp tr', 1, 0.3),
                    ('eq bgp', 1, 0.4),
                    ('eq tr', 1, 0.4),
                    ('bgp tr', 1, 0.4),
                    ('eq bgp tr', 1, 0.5)]
    # all -> mixed mm .. tr
    # + mixed mm -> eq mm .. eq mm bgp tr
    # + mixed no mm -> eq bgp .. eq bgp tr
    mm_fig_sources = [0] * 6 + [1] * 7 + [2] * 4
    mm_fig_targets = list(range(1, len(mm_fig_sources) + 1))
    mm_fig_values = [len(mm_fig_classes[class_name])
                     for class_name, x, y in mm_fig_nodes[1:]]
    for val in mm_fig_values:
        if val == 0:
            print('Error: Can not draw connection with value 0.')
            sys.exit(1)
    labels, x, y = zip(*mm_fig_nodes)
    mm_fig = go.Figure(data=[go.Sankey(valuesuffix=' deps',
                                       arrangement='snap',
                                       node={'label': labels,
                                             'x': x,
                                             'y': y,
                                             'pad': 10},
                                       link={'source': mm_fig_sources,
                                             'target': mm_fig_targets,
                                             'value': mm_fig_values})])
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
    mm_fig.write_image(fig_output_file)

    os.makedirs(data_output_dir, exist_ok=True)
    class_sizes = {class_name: len(dep_set)
                   for class_name, dep_set in class_permutations.items()}
    total_deps = sum(class_sizes.values())

    with open(data_output_file, 'w') as f:
        f.write(DATA_OUTPUT_DELIMITER.join(['class', 'dependencies',
                                            'percentage'])
                + '\n')
        for class_name, dep_count in class_sizes.items():
            dep_percentage = 100 / total_deps * dep_count
            f.write(DATA_OUTPUT_DELIMITER.join(map(str, [class_name,
                                                         dep_count,
                                                         dep_percentage]))
                    + '\n')
        f.write(DATA_OUTPUT_DELIMITER.join(['all', str(total_deps),
                                            '100.0']) + '\n')
    sys.exit(0)

