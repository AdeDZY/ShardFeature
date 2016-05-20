#!/opt/python27/bin/python

import numpy as np
import os
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("partition_name")
    parser.add_argument("--dataset", "-d", choices=["cwb", "gov2"], default="cwb")
    parser.add_argument("--query", "-q", help="print result for each query", action="store_true")
    args = parser.parse_args()

    basedir = "output/{0}/".format(args.partition_name)

    #  shard union features
    union = {}
    nshard = 2000
    for s in range(1, nshard + 1):
        if not os.path.exists(basedir + "/inters/{0}.inter".format(s)):
            nshard = s - 1 
            break
        union[s] = {} 
        with open(basedir + "/inters/{0}.inter".format(s)) as f:
            for line in f:
                qid, i, u = line.split()
                qid = int(qid)
                i = int(i)
                u = int(u)
                union[s][qid] = u


    # Cres: total number of documents
    # Clat: max over shard
    cres = 0
    clat = 0
    cres_ext = 0
    if args.dataset == "cwb":
        n_queries = 200
    else:
        n_queries = 149
    for q in range(n_queries):
        tmp_ext = sum([union[s][q] for s in range(1, nshard + 1)])
        cres_ext += tmp_ext
        tmp = [union[s][q] for s in range(1, nshard + 1)]
        if tmp:
            tmp_res = sum(tmp) 
            tmp_lat, s = max([(v, i + 1) for i, v in enumerate(tmp)])
            if args.query:
                print s, tmp_lat
            cres += tmp_res
            clat += tmp_lat

    print args.partition_name, float(cres_ext)/n_queries, float(cres)/n_queries, float(clat)/n_queries



