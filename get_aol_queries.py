#!/opt/python27/bin/python

import numpy as np
import os
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("org_query_file", type=argparse.FileType('r'))
    parser.add_argument("size_file", type=argparse.FileType('r'))
    parser.add_argument("output_query_file", type=argparse.FileType('w'))
    parser.add_argument("output_qid_file", type=argparse.FileType('w'))
    args = parser.parse_args()

    # read size file
    sizes = {}
    for line in args.size_file:
        size, qid = line.strip().split(' ')
        qid = int(qid.split('.')[0])
        sizes[qid] = int(size)

    qid = 0
    nqid = 0
    queries = set()
    for line in args.org_query_file:
        qid += 1
        if 'http' in line or 'porn' in line or 'sex' in line or 'nude' in line:
            continue
        if sizes.get(qid, 0) < 10000:
            continue
        if any(char.isdigit() for char in line):
            continue
        line = line.strip()
        if line in queries:
            continue
        nqid += 1
        queries.add(line)
        args.output_query_file.write(line + '\n')
        args.output_qid_file.write('{0} {1}\n'.format(qid, nqid))

    args.org_query_file.close()
    args.size_file.close()
    args.output_query_file.close()
    args.output_qid_file.close()
