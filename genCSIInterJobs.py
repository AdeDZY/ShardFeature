#!/opt/python27/bin/python

__author__ = 'zhuyund'

# for a csi
# gen jobs for shardFeature

import argparse
import os
import jobWriter
from os import listdir
from os.path import isfile, join

parser = argparse.ArgumentParser()
parser.add_argument("partition_name")
parser.add_argument("sample_file_path")
parser.add_argument("repo_dir")
parser.add_argument("query_file")
args = parser.parse_args()

base_dir = "/bos/usr0/zhuyund/partition/ShardFeature/output/" + args.partition_name
print base_dir


if not os.path.exists(base_dir):
    os.makedirs(base_dir)

jobs_dir = base_dir + '/jobs'
if not os.path.exists(jobs_dir):
    os.makedirs(jobs_dir)

feat_dir = base_dir + '/inters'
if not os.path.exists(feat_dir):
    os.makedirs(feat_dir)


# preprocssing csi sample file
local_sample_file = base_dir+"/csi_sample"
with open(args.sample_file_path) as f, open(local_sample_file, "w") as fout:
    for line in f:
        extid, shard = line.split(',')
        fout.write(extid)
        fout.write('\n')

execuatable = "./unionInter"
arguments = "{0} {1} {2} {3}".format(args.repo_dir,
                                     local_sample_file,
                                     feat_dir + '/csi.inter',
                                     args.query_file)
log = "/tmp/zhuyund_kmeans.log"
out = base_dir + "/out"
err = base_dir + "/err"

job = jobWriter.jobGenerator(execuatable, arguments, log, err, out)
job_file = open("{0}/csi_inter.job".format(jobs_dir), 'w')
job_file.write(job)
job_file.close()



