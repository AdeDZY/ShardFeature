import numpy as np
import os
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("partition_name")
    parser.add_argument("shardlist_file")
    parser.add_argument("--dataset", "-d", choice=["cwb", "gov2"], default="cwb")
    args = parser.parse_args()

    basedir = "output/{0}/".format(args.partition_name)

    #  shard union features
    union = {}
    nshard = 2000
    for s in range(nshard):
        if not os.path.exists(basedir + "/inters/{0}.inter".format(s + 1)):
            nshard = s
            break
        union[s] = []
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
    for q in shardlist:
        tmp = [union[s][q] for s in shardlist[q]]
        cres += sum(tmp)
        clat += max(tmp)

    print args.partition_name, float(cres)/len(shardlist), float(clat)/len(shardlist)



