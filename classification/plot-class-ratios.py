import argparse
import bz2
import os
import pickle
import sys
from collections import defaultdict
from itertools import permutations

import matplotlib.pyplot as plt
import msgpack
import numpy as np

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
dall = set()
de = set()
dm = set()
db = set()
dt = set()
scope_info = dict()
dep_info = defaultdict(lambda: {'eq': 0, 'mm': 0, 'bgp': 0, 'tr': 0})


def plot_ratio(class_name: str, data: set, output: str, mode: str) -> None:
    x_vals = dict()
    print(class_name)
    classes = class_name.split()
    if len(classes) == 1:
        return
    for class_name in classes:
        if mode == 'scope':
            x_vals[class_name] = [scope_info[scope][class_name]
                                  / sum(scope_info[scope].values())
                                  for scope in data]
        else:
            x_vals[class_name] = [dep_info[dep][class_name]
                                  / sum(dep_info[dep].values())
                                  for dep in data]
    for class_name in x_vals:
        print(f'{class_name}: {len(x_vals[class_name])}')
    p_vals = None
    for x_val in x_vals.values():
        x_val.sort()
        if p_vals is None:
            p_vals = (np.arange(len(x_val)) + 1) / len(x_val)

    fa = plt.subplots()
    ax: plt.Axes = fa[1]
    for class_name, x_val in x_vals.items():
        ax.plot(x_val, p_vals, '-', label=class_name)
    ax.legend()

    ax.set_ylim(0, 1)
    ax.set_yticks(np.arange(0, 1.1, 0.1))
    ax.set_ylabel('p')
    ax.set_xlim(0, 1)
    ax.set_xticks(np.arange(0, 1.1, 0.1))
    ax.set_xlabel('ratio')
    ax.grid()
    # plt.show()
    plt.savefig(output, bbox_inches='tight')


def check_bias(combined_class_name: str,
               data: set,
               mode: str,
               threshold: float) -> dict:
    classes = combined_class_name.split()
    if len(classes) == 1:
        return dict()
    ratios = dict()
    for class_name in classes:
        if mode == 'scope':
            ratios[class_name] = [(scope_info[scope][class_name]
                                   / sum(scope_info[scope].values(), scope))
                                  for scope in data]
        else:
            ratios[class_name] = [(dep_info[dep][class_name]
                                   / sum(dep_info[dep].values()), dep)
                                  for dep in data]
    res = dict()
    for class_name, ratio in ratios.items():
        filtered_ratios = [r for r in ratio if r[0] >= threshold]
        if not filtered_ratios:
            continue
        filtered_ratios.sort()
        res[class_name] = filtered_ratios
    return res


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('topic', help='*.pickle.bz2 dump of topic')
    parser.add_argument('-f', '--fig-output', help='Figure output directory',
                        default='./')
    parser.add_argument('-d', '--data-output', help='Data output directory',
                        default='./')
    parser.add_argument('-b', '--bias-threshold', type=float)

    args = parser.parse_args()

    fig_output_dir: str = args.fig_output
    if not fig_output_dir.endswith('/'):
        fig_output_dir += '/'

    data_output_dir: str = args.data_output
    if not data_output_dir.endswith('/'):
        data_output_dir += '/'

    bias_threshold = args.bias_threshold

    if not args.topic.endswith(INPUT_EXTENSION):
        print(f'Error: Expected {INPUT_EXTENSION} input file, but got '
              f'{args.topic}', file=sys.stderr)
        sys.exit(1)

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
        scope_info[scope] = {'eq': len(msg_data['equal']),
                             'mm': len(msg_data['mismatched']),
                             'bgp': len(msg_data['bgp_only']),
                             'tr': len(msg_data['tr_only'])}
        for asn, bgp_score, bgp_rank, tr_score, tr_rank, comp_rank \
                in msg_data['equal']:
            if asn not in dall:
                dall.add(asn)
            if asn not in de:
                de.add(asn)
            dep_info[asn]['eq'] += 1
        for asn, bgp_score, bgp_rank, bgp_comp_rank, tr_score, tr_rank, \
            tr_comp_rank in msg_data['mismatched']:
            if asn not in dall:
                dall.add(asn)
            if asn not in dm:
                dm.add(asn)
            dep_info[asn]['mm'] += 1
        for asn, score, rank in msg_data['bgp_only']:
            if asn not in dall:
                dall.add(asn)
            if asn not in db:
                db.add(asn)
            dep_info[asn]['bgp'] += 1
        for asn, score, rank in msg_data['tr_only']:
            if asn not in dall:
                dall.add(asn)
            if asn not in dt:
                dt.add(asn)
            dep_info[asn]['tr'] += 1

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

    output_file_prefix = os.path.basename(args.topic)[:-len(INPUT_EXTENSION)]

    os.makedirs(fig_output_dir, exist_ok=True)

    sclasses = spermutator.get_permutations()
    print('\nscopes')
    for class_name, scopes in sclasses.items():
        fig_output_file = fig_output_dir + 'class_ratios_scopes.' + \
                          class_name.replace(' ', '_') + '.' + \
                          output_file_prefix + FIG_OUTPUT_EXTENSION
        plot_ratio(class_name, scopes, fig_output_file, 'scope')

    dclasses = dpermutator.get_permutations()
    print('\ndependencies')
    biased = dict()
    for class_name, dependencies in dclasses.items():
        fig_output_file = fig_output_dir + 'class_ratios_dependencies.' + \
                          class_name.replace(' ', '_') + '.' + \
                          output_file_prefix + FIG_OUTPUT_EXTENSION
        plot_ratio(class_name, dependencies, fig_output_file, 'dependency')
        if bias_threshold:
            bias = check_bias(class_name, dependencies, 'dependency',
                              bias_threshold)
            if bias:
                biased[class_name] = bias
    if bias_threshold:
        bias_file = data_output_dir + 'biased_dependencies_' + \
                    str(bias_threshold) + DATA_OUTPUT_EXTENSION
        for class_name, bias in biased.items():
            print(class_name)
            for subclass, entries in bias.items():
                print(subclass, entries)
            print()

    sys.exit(0)
