import argparse
import os
import sys
from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np

DATA_EXTENSION = '.csv'
DATA_DELIMITER = ','


def get_data_from_file(file: str, sampling_value: int, total_data: dict) -> dict:
    with open(file, 'r') as f:
        # Skip headers
        f.readline()
        for line in f:
            line_split = line.split(DATA_DELIMITER)
            if len(line_split) < 3:
                print(f'Error: Malformed data line: {line.strip()}',
                      file=sys.stderr)
                continue
            try:
                scope = int(line_split[0])
                asn = int(line_split[1])
                score = float(line_split[2])
            except ValueError as e:
                print(f'Error: Malformed data line: {line.strip()}: {e}',
                      file=sys.stderr)
                continue
            total_data[scope][asn][sampling_value].append(score)
    return total_data


def load_per_sampling_value_data(path: str,
                                 sampling_value: int,
                                 total_data: dict) -> dict:
    for entry in os.scandir(path):
        if not entry.is_file() or not entry.name.endswith(DATA_EXTENSION):
            continue
        total_data = get_data_from_file(path + entry.name,
                                        sampling_value,
                                        total_data)
    return total_data


def load_data(path: str, values: set = None) -> dict:
    # What a monster.
    # Structure is scope -> asn -> sampling_value -> list of scores
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    sampling_steps = list()
    for entry in os.scandir(path):
        if not entry.is_dir() or not entry.name.isdigit():
            continue
        if values and entry.name not in values:
            continue
        sampling_value = int(entry.name)
        if sampling_value < 10:
            continue
        sampling_steps.append(sampling_value)
        data = load_per_sampling_value_data(path + entry.name + '/',
                                            sampling_value,
                                            data)
    # Ensure that all sampling percentages are present for all AS
    # dependencies.
    for scope in data:
        for asn in data[scope]:
            for sampling_value in sampling_steps:
                if sampling_value not in data[scope][asn]:
                    data[scope][asn][sampling_value] = [0]
    return data


def fill_missing_values(data: dict, iterations: int) -> None:
    # In order to get representative median values, we need to keep
    # track of AS dependencies that are only present in some iterations.
    # To do this, we normalize the size of the score list of each
    # dependency to the number of iterations by filling up missing
    # values with zero, indicating an absence in some iterations.
    for scope in data:
        for asn in data[scope]:
            for sampling_value in data[scope][asn]:
                # Don't extend this, since we have only one iteration
                # by design.
                # if sampling_value == 100:
                #     continue
                score_list = data[scope][asn][sampling_value]
                if len(score_list) > iterations:
                    print(f'Error: Score list has more than #iterations '
                          f'({iterations}) entries: {len(score_list)}',
                          file=sys.stderr)
                    print(f'       scope: {scope} asn: {asn} sampling '
                          f'percentage: {sampling_value}')
                    continue
                elif len(score_list) < iterations:
                    score_list += [0] * (iterations - len(score_list))


def strip_empty_sampling_values(data: dict) -> None:
    # Hackish way to get set of sampling values.
    if not data:
        return
    random_scope = list(data.keys())[0]
    if not data[random_scope]:
        return
    random_asn = list(data[random_scope].keys())[0]
    sampling_values = set(data[random_scope][random_asn].keys())

    # For each scope, check sampling values in descending order. If
    # there are no scores for all dependencies for a sampling value, the
    # value can be removed. Since we only want to trim the top values,
    # stop as soon as there exists a score for any dependency.
    for scope in data:
        for sampling_value in sorted(sampling_values, reverse=True):
            all_empty = True
            for asn in data[scope]:
                if any(score != 0
                       for score in data[scope][asn][sampling_value]):
                    all_empty = False
                    break
            if all_empty:
                for asn in data[scope]:
                    data[scope][asn].pop(sampling_value)
            else:
                break


def plot_scope(scope: int, scope_data: dict, output_dir: str) -> None:
    asns = sorted(scope_data.keys())

    markerstyle = {'markersize': 5,
                   'marker': '.'}

    num_dependencies = len(asns)
    cols = min(3, num_dependencies)
    rows = int(np.ceil(num_dependencies / cols))
    fa = plt.subplots(rows, cols, sharex='col')
    fig: plt.Figure = fa[0]
    axes = fa[1].flat
    for ax in axes[num_dependencies:]:
        ax.remove()
    fig.set_size_inches(cols * 3, rows * 2)
    for asn_idx in range(num_dependencies):
        ax: plt.Axes = axes[asn_idx]
        ax.set_title(str(asns[asn_idx]))
        scores = list()
        labels = list()
        for sampling_value in sorted(scope_data[asns[asn_idx]].keys()):
            scores.append(scope_data[asns[asn_idx]][sampling_value])
            labels.append(str(sampling_value))
        if len(labels) > 20:
            ax.boxplot(scores, flierprops=markerstyle,
                       labels=[l if (i + 1) % 2 else '' for i, l in enumerate(labels)])
        else:
            ax.boxplot(scores, flierprops=markerstyle,
                       labels=labels)
        # ax.violinplot(scores)
        ax.set_ylim(ymin=0)
        # Add x label to last row
        if asn_idx >= num_dependencies - cols:
            ax.tick_params(axis='x', labelrotation=90)
            ax.set_xlabel('Sampling value')
        # Add y label to first column
        if asn_idx % cols == 0:
            ax.set_ylabel('Hegemony score')

    fig.suptitle(f'Scope {scope}')
    fig.tight_layout()
    plt.savefig(output_dir + str(scope) + '.pdf', bbox_inches='tight')
    # plt.show()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('data_dir')
    parser.add_argument('iterations', type=int)
    parser.add_argument('-v', '--values', type=lambda l: set(l.split(',')),
                        help='comma-separated list of values to plot')
    parser.add_argument('-o', '--output', default='./',
                        help='Output directory (default: ./)')
    args = parser.parse_args()

    data_dir = args.data_dir
    if not data_dir.endswith('/'):
        data_dir += '/'

    output_dir = args.output
    if not output_dir.endswith('/'):
        output_dir += '/'

    data = load_data(data_dir, args.values)
    fill_missing_values(data, args.iterations)
    strip_empty_sampling_values(data)

    for scope in data:
        print(scope)
        plot_scope(scope, data[scope], output_dir)


if __name__ == '__main__':
    main()
    sys.exit(0)
