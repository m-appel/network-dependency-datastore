import argparse
import bz2
import os
import pickle
import sys

import msgpack

INPUT_EXTENSION = '.pickle.bz2'
OUTPUT_EXTENSION = '.csv'
OUTPUT_DELIMITER = ','


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='*.pickle.bz2 topic dump')
    parser.add_argument('-o', '--output_dir', help='specify output directory')

    args = parser.parse_args()

    path, file = os.path.split(args.input)
    if not file.endswith(INPUT_EXTENSION):
        print(f'Warning: Unexpected extension for input file. Will append '
              f'{OUTPUT_EXTENSION} to full name.')
        file += OUTPUT_EXTENSION
    else:
        file = file[:-len(INPUT_EXTENSION)] + OUTPUT_EXTENSION

    if args.output_dir:
        output_dir = args.output_dir
        if not output_dir.endswith('/'):
            output_dir += '/'
        output_file = output_dir + file
    else:
        output_file = path + '/' + file

    print(f'Reading file {args.input}')
    data = pickle.load(bz2.BZ2File(args.input, 'rb'))
    messages = [msgpack.unpackb(msg[1]) for msg in data['messages']]

    print(f'Writing output to {output_file}')
    with open(output_file, 'w') as f:
        f.write(OUTPUT_DELIMITER.join(['scope', 'asn', 'hege', 'nb_peers'])
                + '\n')
        for msg in messages:
            if msg['scope'] == '-1' or msg['scope'] == msg['asn']:
                continue
            fields = [msg['scope'], msg['asn'], msg['hege'], msg['nb_peers']]
            f.write(OUTPUT_DELIMITER.join(map(str, fields)) + '\n')


if __name__ == '__main__':
    main()
    sys.exit(0)
