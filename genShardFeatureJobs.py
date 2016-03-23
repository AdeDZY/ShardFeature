#!/opt/python27/bin/python

__author__ = 'zhuyund'

# for a partition
# gen jobs for shardFeature


import argparse
import os, sys
import jobWriter
from os import listdir
from os.path import isfile, join

parser = argparse.ArgumentParser()
parser.add_argument("partition_name")
parser.add_argument("shardmaps_dir")
parser.add_argument("repo_dir")
parser.add_argument("query_term_file")
parser.add_argument("--bigram", "-b", action="store_true")
parser.add_argument("--fielded", action="store_true")
parser.add_argument("--nospam", "-s", action="store_true")
parser.add_argument("--spamfile", "-f", type=str, default="/bos/usr0/zhuyund/fedsearch/cw09b.spam")
args = parser.parse_args()

base_dir = "/bos/usr0/zhuyund/partition/ShardFeature/output/" + args.partition_name
print base_dir


if not os.path.exists(base_dir):
    os.makedirs(base_dir)

jobs_dir = base_dir + '/jobs'
if not os.path.exists(jobs_dir):
    os.makedirs(jobs_dir)

feat_dir = base_dir + '/features'
if not os.path.exists(feat_dir):
    os.makedirs(feat_dir)

shardmaps = [f.strip() for f in listdir(args.shardmaps_dir)
             if isfile(join(args.shardmaps_dir, f)) and f.isdigit()]

shardmap_file = open(base_dir + "/shards", 'w')
for f in shardmaps:
    shardmap_file.write(f + '\n')
shardmap_file.close()

for shardmap in shardmaps:
    execuatable = "./shardFeature"
    arguments = "{0} {1} {2} {3}".format(args.repo_dir,
                                         args.shardmaps_dir + '/' + shardmap,
                                         feat_dir + '/' + shardmap + '.feat',
                                         args.query_term_file)
    if args.nospam:
        execuatable = "./shardFeature_nospam"
        arguments = "{0} {1} {2} {3} {4}".format(args.repo_dir,
                                         args.shardmaps_dir + '/' + shardmap,
                                         feat_dir + '/' + shardmap + '.feat',
                                         args.query_term_file,
                                         args.spamfile)
    if args.bigram:
        execuatable = "./shardFeature_bigram"
        arguments = "{0} {1} {2} {3}".format(args.repo_dir,
                                         args.shardmaps_dir + '/' + shardmap,
                                         feat_dir + '/' + shardmap + '.feat_bigram',
                                         args.query_term_file)
    if args.fielded:
        execuatable = "./shardFeature_fielded"
        arguments = "{0} {1} {2} {3}".format(args.repo_dir,
                                         args.shardmaps_dir + '/' + shardmap,
                                         feat_dir + '/' + shardmap + '.feat_fielded',
                                         args.query_term_file)
    log = "/tmp/zhuyund_kmeans.log"
    out = base_dir + "/out"
    err = base_dir + "/err"

    job = jobWriter.jobGenerator(execuatable, arguments, log, err, out)
    job_file = open("{0}/{1}_feat.job".format(jobs_dir, shardmap), 'w')
    job_file.write(job)
    job_file.close()



