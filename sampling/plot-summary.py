import argparse
import os
import sys
from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np

DATA_EXTENSION = '.csv'
DATA_DELIMITER = ','
REFERENCE_DIR_NAME = 'ref'


def get_data_from_file(file: str,
                       total_data: dict,
                       sampling_value: int) -> dict:
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
            if sampling_value < 0:
                # Used for reference data
                total_data[scope][asn] = score
            else:
                total_data[scope][asn][sampling_value].append(score)
    return total_data


def load_per_sampling_value_data(path: str,
                                 total_data: dict,
                                 sampling_value: int = -1) -> dict:
    for entry in os.scandir(path):
        if not entry.is_file() or not entry.name.endswith(DATA_EXTENSION):
            continue
        total_data = get_data_from_file(path + entry.name, total_data,
                                        sampling_value)
    return total_data


def load_data(path: str, values: set = None) -> (dict, dict):
    # What a monster.
    # Structure is scope -> asn -> sampling_value -> list of scores
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    # Reference data only has one score per asn per scope.
    reference_data = defaultdict(dict)
    sampling_steps = list()
    for entry in os.scandir(path):
        if not entry.is_dir():
            continue
        sampling_path = path + entry.name + '/'
        if entry.name == REFERENCE_DIR_NAME:
            reference_data = load_per_sampling_value_data(sampling_path,
                                                          reference_data)
            continue
        if not entry.name.isdigit():
            continue
        if values and entry.name not in values:
            continue
        sampling_value = int(entry.name)
        sampling_steps.append(sampling_value)
        data = load_per_sampling_value_data(sampling_path, data, sampling_value)
    # Ensure that all sampling percentages are present for all AS
    # dependencies.
    for scope in data:
        for asn in data[scope]:
            for sampling_value in sampling_steps:
                if sampling_value not in data[scope][asn]:
                    data[scope][asn][sampling_value] = [0]
    return reference_data, data


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


def get_diffs(ref_data: dict, scope_data: dict) -> dict:
    # Map sampling value to list of diffs for all ASes
    ret = defaultdict(lambda: defaultdict(list))
    asns_per_sample = defaultdict(set)
    for asn in scope_data:
        if asn not in ref_data:
            # print(f'Warning: AS {asn} not present in reference data',
            #       file=sys.stderr)
            ref_score = 0
        else:
            ref_score = ref_data[asn]
        for sampling_value in scope_data[asn]:
            for score in scope_data[asn][sampling_value]:
                if ref_score == score == 0:
                    # No ref_score exists and score is a filler value
                    # so do not count as a difference.
                    continue
                ret[sampling_value][asn].append(abs(ref_score - score))
    return ret


def filter_dependencies(scope_data: dict, percentile: int) -> None:
    filtered_asns = list()
    for asn in scope_data:
        filtered_sampling_values = list()
        for sampling_value in scope_data[asn]:
            if np.percentile(scope_data[asn][sampling_value], percentile, interpolation='lower') == 0:
                print(f'Removing samples {scope_data[asn][sampling_value]}: {np.percentile(scope_data[asn][sampling_value], percentile, interpolation="lower")}')
            # if np.median(scope_data[asn][sampling_value]) == 0:
                filtered_sampling_values.append(sampling_value)
        if filtered_sampling_values:
            print(f'Removing samples {filtered_sampling_values} from dependency {asn}')
        for sampling_value in filtered_sampling_values:
            del scope_data[asn][sampling_value]
        if not scope_data[asn]:
            filtered_asns.append(asn)
    if filtered_asns:
        print(f'Removing dependencies entirely {filtered_asns}')
    for asn in filtered_asns:
        del scope_data[asn]


def plot_scope(scope: str, scope_diffs: dict, output_dir: str) -> None:
    fa = plt.subplots()
    fig: plt.Figure = fa[0]
    ax: plt.Axes = fa[1]

    ax.set_xscale('log')

    ax2: plt.Axes = ax.twinx()
    ax2.set_yscale('log')

    scores = list()
    labels = list()
    x_vals = list()
    mins = list()
    # percentiles = [50]
    # percentile_values = list()
    medians = list()
    maxs = list()
    asns = list()
    for sampling_value in sorted(scope_diffs.keys()):
        x_vals.append(sampling_value)
        labels.append(str(sampling_value))
        asns.append(len(scope_diffs[sampling_value]))
        scores = list()
        for asn_scores in scope_diffs[sampling_value].values():
            scores += asn_scores
        if not scores:
            print(f'No dependencies left for sample {sampling_value}')
            mins.append(0)
            medians.append(0)
            maxs.append(0)
            continue
        mins.append(np.min(scores))
        medians.append(np.median(scores))
        # percentile_values.append(np.percentile(scope_diffs[sampling_value], percentiles))
        maxs.append(np.max(scores))
        # scores.append(scope_diffs[sampling_value])

    # ax.boxplot(scores, labels=[l if (i + 1) % 2 else '' for i, l in enumerate(labels)])
    # ax.boxplot(scores, labels=labels)
    # for idx, p in enumerate(zip(*percentile_values)):
    #     ax.plot(x_vals, p, label=percentiles[idx])
    med_line, = ax.plot(x_vals, medians)
    ax.fill_between(x_vals, mins, maxs, alpha=0.3)
    dep_line, = ax2.plot(x_vals, asns, c='red')

    ax.set_title(f'Scope {scope}')
    ax.tick_params(axis='x', labelrotation=20)
    ax.set_xlabel('Sampling value')
    ax.set_xlim(xmin=2)
    ax.set_ylim(0, 1.05)
    ax.set_yticks(np.arange(0, 1.1, 0.1))
    ax.set_ylabel('Hegemony score difference')
    ax.grid(axis='y', ls='--')

    ax2.set_ylim(ymin=1)
    ax2.set_ylabel('Dependencies')
    ax.legend([med_line, dep_line], ['median', 'dependencies'])

    fig.tight_layout()
    plt.savefig(output_dir + scope + '-summary.pdf', bbox_inches='tight')
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

    ref_data, data = load_data(data_dir, args.values)
    fill_missing_values(data, args.iterations)
    strip_empty_sampling_values(data)

    global_diffs = defaultdict(lambda: defaultdict(list))

    for scope in data:
        print(f'Scope: {scope}')
        if scope not in ref_data:
            print(f'Error: Missing reference data for scope {scope}',
                  file=sys.stderr)
            continue
        filter_dependencies(data[scope], 90)
        scope_diffs = get_diffs(ref_data[scope], data[scope])
        plot_scope(str(scope), scope_diffs, output_dir)
        for sampling_value in scope_diffs:
            for asn in scope_diffs[sampling_value]:
                global_diffs[sampling_value][asn] += scope_diffs[sampling_value][asn]
    print('Global scope')
    plot_scope('global', global_diffs, output_dir)


if __name__ == '__main__':
    main()
    sys.exit(0)
