#!/opt/python27/bin/python

__author__ = 'zhuyund'

import argparse
import os, sys
import jobWriter
from os import listdir
from os.path import isfile, join
from numpy import log2, log

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("partition_name")
    parser.add_argument("csi_dir")
    parser.add_argument("n_shard", type=int)
    parser.add_argument("--dataset", "-d", type=str, choices=["cwb", "gov2"], default="cwb")
    args = parser.parse_args()

    base_dir = "/bos/usr0/zhuyund/partition/ShardFeature/output/" + args.partition_name
    print base_dir

    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
    feat_dir = join(base_dir, "csi_features")

    if args.dataset == "cwb":
        queries = range(1, 201)
    elif args.dataset == "gov2":
        queries = range(701, 851)

    # for each query, read in its csi filtered file
    out_file = open(join(feat_dir, "all.feat"), 'w')
    for q in queries:
        filtered_file_path = join(args.csi_idr, "{0}.filtered".format(q))
        filtered = []
        if not os.path.exists(filtered_file_path):
            continue
        outputs = [str(q)]
        r = 0
        with open(filtered_file_path) as f:
            for line in f:
                r += 1
                shardid = int(line.strip().split()[-1])
                filtered.append((shardid, r))

        # union
        outputs.append(str(r))

        # entropy and rel_entropy
        tmp = [0 for i in range(args.n_shard)]
        for shardid, r in tmp:
            tmp[shardid - 1] += 1.0/r
        h = 0
        rel_h = 0
        for i in range(args.n_shard):
            if tmp[i] > 0:
                h -= tmp[i] * log2(tmp[i])
                rel_h += tmp[i] * log(tmp[i] * r)
        outputs.append(str(h))
        outputs.append(str(rel_h))

        out_file.write('\t'.join(outputs))
        out_file.write('\n')


