#!/opt/python27/bin/python
__author__ = 'zhuyund'

import argparse
import os
import time

parser = argparse.ArgumentParser()
parser.add_argument("partition_name")
parser.add_argument("job_type", type=int, help="1:getShardFeature 2:unionInter 3:bigram 4:fielded")
parser.add_argument("--sleep", "-s", type=int, help="sleep time in seconds", default=90)
parser.add_argument("--nbatch", "-n", type=int, help="submit n batches at one time", default=1)
parser.add_argument("--start", "-t", type=int, help="start from this line of shard file", default=1)
parser.add_argument("--end", "-e", type=int, help="submit to this line of shard file", default=10000)
args = parser.parse_args()

base_dir = "/bos/usr0/zhuyund/partition/ShardFeature/output/" + args.partition_name

n = 0
n_line = 0
for line in open(base_dir + "/shards"):
    shard = line.strip().split()[0]
    n_line += 1
    if n_line < args.start:
        continue
    if n_line > args.end:
        break
    if args.job_type == 1:
        job_path = base_dir + "/jobs/{0}_feat.job".format(shard)
    elif args.job_type == 2:
        job_path = base_dir + "/jobs/{0}_inter.job".format(shard)
    elif args.job_type == 3:
        job_path = base_dir + "/jobs/{0}_bigram.job".format(shard)
    elif args.job_type == 4:
        job_path = base_dir + "/jobs/{0}_fielded.job".format(shard)
    else:
        print "wrong job type!"

    while True:
        if args.job_type == 1:
            query = "condor_q zhuyund | grep " + "\"" + "shardFeature" + "\"" + "| wc -l"
        elif args.job_type == 2:
            query = "condor_q zhuyund | grep " + "\"" + "unionInter" + "\"" + "| wc -l"
        elif args.job_type == 3:
            query = "condor_q zhuyund | grep " + "\"" + "shardFeature_bi" + "\"" + "| wc -l"
        elif args.job_type == 4:
            query = "condor_q zhuyund | grep " + "\"" + "shardFeature_fi" + "\"" + "| wc -l"
        out = os.popen(query)
        nRunning = int(out.readline())
        print nRunning
        if nRunning < 12:
            break
        time.sleep(args.sleep)
    cmd = "condor_submit " + job_path
    os.system(cmd)
    n += 1
    if n % args.nbatch == 0:
        time.sleep(args.sleep)
