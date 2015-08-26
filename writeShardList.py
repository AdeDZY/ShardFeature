#!/opt/python27/bin/python

__author__ = 'zhuyund'
import argparse
import argparse
import os, sys
import jobWriter
from os import listdir
from os.path import isfile, join


parser = argparse.ArgumentParser()
parser.add_argument("partition_name")
parser.add_argument("shardlim", type=int, help="maximum number of shards to be selected")
parser.add_argument("miu", type=float)
args = parser.parse_args()

base_dir = "/bos/usr0/zhuyund/partition/ShardFeature/output/" + args.partition_name

rankings_dir = base_dir + "/rankings/"

run_dir = "/bos/usr0/zhuyund/fedsearch/output/rankings/cent/{0}/lim{1}_miu{2}/".format(args.partition_name,
                                                                                       args.shardlim,
                                                                                       args.miu)
if not os.path.exists(run_dir):
    os.makedirs(run_dir)

shardlist_file = open("{0}/all.shardlist".format(run_dir), 'w')


qids = [f.strip().split('.')[0] for f in listdir(rankings_dir) if isfile(join(rankings_dir, f))]

for qid in qids:
    ranking = open("{0}/{1}.rank".format(rankings_dir, qid))
    shards = []
    for i, line in enumerate(ranking):
        shard, score = line.split()
        shards.append(shard)
        if i == args.shardlim - 1:
            break
    ranking.close()
    shardlist_file.write(qid + ' ' + ' '.join(shards))
    shardlist_file.write('\n')

shardlist_file.close()



