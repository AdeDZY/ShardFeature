#!/opt/python27/bin/python

import numpy as np
import os
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("partition_name")
    parser.add_argument("shardlist_file")
    parser.add_argument("--dataset", "-d", choices=["cwb", "gov2"], default="cwb")
    parser.add_argument("--query", "-q", help="print result for each query")
    args = parser.parse_args()

    basedir = "output/{0}/".format(args.partition_name)

    #  shard union features
    union = {}
    nshard = 2000
    for s in range(nshard):
        if not os.path.exists(basedir + "/inters/{0}.inter".format(s + 1)):
            nshard = s
            break
        union[s] = {} 
        with open(basedir + "/inters/{0}.inter".format(s + 1)) as f:
            for line in f:
                qid, i, u = line.split()
                qid = int(qid)
                i = int(i)
                u = int(u)
                union[s][qid] = u

    #  read shard list
    shardlist = {}
    with open(args.shardlist_file) as f:
        for line in f:
            items = line.split()
            if args.dataset == "cwb":
                q = int(items[0]) - 1
            else:
                q = int(items[0])
                if q < 803:
                    q -= 701
                elif q == 803:
                    continue
                else:
                    q -= 702
            shardlist[q] = []
            for t in items[1:]:
                shardlist[q].append(int(t) - 1)

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
        tmp = []
        if q in shardlist:
            tmp = [union[s][q] for s in shardlist[q] if s in union]
        tmp_ext = sum([union[s][q] for s in range(nshard)])
        cres_ext += tmp_ext

        tmp.append(0)
        tmp_res = sum(tmp)
        tmp_lat = map(tmp)
        if args.query:
            print tmp_ext, tmp_res, tmp_lat
        cres += tmp_res
        clat += tmp_lat

    print args.partition_name, float(cres_ext)/n_queries, float(cres)/n_queries, float(clat)/n_queries



