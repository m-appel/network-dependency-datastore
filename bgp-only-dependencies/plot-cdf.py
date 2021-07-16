import argparse
import bz2
import os
import pickle
import sys
from datetime import datetime, timezone

import matplotlib.pyplot as plt
import msgpack
import numpy as np

DATE_FMT = '%Y-%m-%dT%H:%M:%S'
INPUT_EXTENSION = '.pickle.bz2'
DATA_OUTPUT_EXTENSION = '.csv'
DATA_OUTPUT_DELIMITER = ','
FIG_OUTPUT_EXTENSION = '.svg'


def main() -> None:
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

    data_output_dir = args.data_output
    if not data_output_dir.endswith('/'):
        data_output_dir += '/'

    output_file_prefix = os.path.basename(args.topic)[:-len(INPUT_EXTENSION)]
    fig_output_file = fig_output_dir + 'bgp-only-visibility.' + \
                      output_file_prefix + FIG_OUTPUT_EXTENSION

    if not args.topic.endswith(INPUT_EXTENSION):
        print(f'Error: Expected {INPUT_EXTENSION} input file, but got '
              f'{args.topic}', file=sys.stderr)
        sys.exit(1)

    with bz2.open(args.topic, 'r') as f:
        data = pickle.load(f)
    x_vals = list()
    for msg in data['messages']:
        msg_data = msgpack.loads(msg[1])
        x_vals.append(msg_data['unique_ips'])
    x_vals.sort()
    p_vals = (np.arange(len(x_vals)) + 1) / len(x_vals)

    fa = plt.subplots()
    ax: plt.Axes = fa[1]

    ax.plot(x_vals, p_vals)
    ax.set_ylim(0, 1)
    ax.set_yticks(np.arange(0, 1.1, 0.1))
    ax.set_ylabel('p')

    x_lim_max = 50
    x_tick_spacing = 10
    ax.set_xlim(-1, x_lim_max)
    ax.set_xticks(np.arange(0, x_lim_max + 1, x_tick_spacing), minor=False)
    minor_ticks = np.concatenate((np.arange(0, 10, 1),
                                  np.arange(10, x_lim_max + 1, x_tick_spacing)))
    ax.set_xticks(minor_ticks, minor=True)
    ax.set_xlabel('unique IPs')

    title = datetime.fromtimestamp(data['start_ts'] / 1000, tz=timezone.utc) \
        .strftime(DATE_FMT)
    ax.set_title(title)

    ax.grid(which='both')

    os.makedirs(fig_output_dir, exist_ok=True)

    # plt.show()
    plt.savefig(fig_output_file, bbox_inches='tight')


if __name__ == '__main__':
    main()
    sys.exit(0)
