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

sall = set()
se = set()
sm = set()
sb = set()
st = set()

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
    data_output_file = data_output_dir + 'scopes.' + output_file_prefix + \
                       DATA_OUTPUT_EXTENSION
    fig_output_file = fig_output_dir + 'scopes.' + output_file_prefix + \
                      FIG_OUTPUT_EXTENSION

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
    permutator = SetPermutator()
    permutator.add_class('eq', se)
    permutator.add_class('mm', sm)
    permutator.add_class('bgp', sb)
    permutator.add_class('tr', st)
    class_permutations = permutator.get_permutations()
    # Sanity checks
    for a, b in permutations(class_permutations.values(), 2):
        if not a.isdisjoint(b):
            print('Error: Scope sets are not disjoint')
            sys.exit(1)
    all_check = set()
    for n, s in class_permutations.items():
        all_check.update(s)
    if sall != all_check:
        print(f'Error: Union of separate sets is missing scopes: '
              f'{sall - all_check}')
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
                    ('eq', 0.5, 0.5),
                    ('mm', 0.5, 0.6),
                    # bgp and tr are zero
                    # ('bgp', 0.5, 0.7),
                    # ('tr', 0.5, 0.8),
                    ('mixed mm', 0.5, 0.1),
                    ('mixed no mm', 0.5, 0.4),
                    ('eq mm', 1, 0.1),
                    ('mm bgp', 1, 0.1),
                    ('mm tr', 1, 0.1),
                    ('eq mm bgp', 1, 0.1),
                    ('eq mm tr', 1, 0.1),
                    ('mm bgp tr', 1, 0.1),
                    ('eq mm bgp tr', 1, 0.1),
                    ('eq bgp', 1, 0.5),
                    ('eq tr', 1, 0.5),
                    ('bgp tr', 1, 0.5),
                    ('eq bgp tr', 1, 0.4)]
    # all -> eq .. mixed no mm
    # + mixed mm -> eq mm .. eq mm bgp tr
    # + mixed no mm -> eq bgp .. eq bgp tr
    mm_fig_sources = [0] * 4 + [3] * 7 + [4] * 4
    mm_fig_targets = list(range(1, len(mm_fig_sources) + 1))
    mm_fig_values = [len(mm_fig_classes[class_name])
                     for class_name, x, y in mm_fig_nodes[1:]]
    for val in mm_fig_values:
        if val == 0:
            print('Error: Can not draw connection with value 0.')
            sys.exit(1)
    labels, x, y = zip(*mm_fig_nodes)
    mm_fig = go.Figure(data=[go.Sankey(valuesuffix=' scopes',
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
    mm_fig.write_image(fig_output_file, width=1000, height=800)

    os.makedirs(data_output_dir, exist_ok=True)
    class_sizes = {class_name: len(scope_set)
                   for class_name, scope_set in class_permutations.items()}
    total_scopes = sum(class_sizes.values())

    with open(data_output_file, 'w') as f:
        f.write(DATA_OUTPUT_DELIMITER.join(['class', 'scopes', 'percentage'])
                + '\n')
        for class_name, scope_count in class_sizes.items():
            scope_percentage = 100 / total_scopes * scope_count
            f.write(DATA_OUTPUT_DELIMITER.join(map(str, [class_name,
                                                         scope_count,
                                                         scope_percentage]))
                    + '\n')
        f.write(DATA_OUTPUT_DELIMITER.join(['all', str(total_scopes),
                                            '100.0']) + '\n')
    sys.exit(0)
