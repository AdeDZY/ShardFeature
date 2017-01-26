#!/opt/python27/bin/python
import argparse
import os
import numpy as np


def read_feat_file(filepath):
    term2feat = {}
    shard_size = 0
    shard_tf = 0
    for line in open(filepath):
        t, df, sum_tf, sum_prob = line.split()
        t = t.strip()
        if '-1' in t :
            shard_size = int(df)
            shard_tf = int(sum_tf)
            continue
        if shard_size == 0:
            print filepath
        p = float(sum_prob) / shard_size
        term2feat[t] = (int(df), int(sum_tf), p)
    return term2feat, shard_size, shard_tf


def gen_cori_lst(shards_features, query, shards_size):
    """
    gen cori features
    :param shards_features:
    :param query:
    :param shards_size:
    :return:
    """
    qterms = query.split()
    res = []
    C = len(shards_features)
    avg_coll_len = sum(shards_size.values())/float(C)

    # cf: resources containing q_i
    cf = []
    for token in qterms:
        cf_i = len([feat for feat in shards_features.values() if token in feat])
        cf.append(cf_i)

    for shard in shards_features:
        feat = shards_features[shard]
        coll_len_j = shards_size[shard]
        P_q_rj = 0
        for i, token in enumerate(qterms):
            if token in feat:
                df = float(feat[token][0])
            else:
                df = 0.0
            P_qi_rj = df/(df + 50 + 150 * coll_len_j/avg_coll_len) * np.log((C + 0.5)/cf[i]) / np.log(C + 1)
            P_q_rj += P_qi_rj
        res.append((P_q_rj, shard))

    sorted_res = sorted(res, reverse=True)
    return sorted_res


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("partition_name")
    parser.add_argument("int_query_file", type=argparse.FileType('r'), help="queries in int format (queryid:queryterms)")

    args = parser.parse_args()

    base_dir = "/bos/usr0/zhuyund/partition/ShardFeature/output/" + args.partition_name

    queries = []
    for query in args.int_query_file:
        query = query.strip()
        query_id, query = query.split(":")
        queries.append((query_id, query))

    res_dir = base_dir + "/rankings/"
    if not os.path.exists(res_dir):
        os.makedirs(res_dir)

    shard_file = base_dir + "/shards"
    shards = []
    for line in open(shard_file):
        shards.append(line.strip())

    # read in all feature files
    shards_features = {}
    shards_size = {}
    shards_tf = {}

    field = ""
    if args.field:
        field = '_' + args.field

    for shard in shards:
        feat_file_path = "{0}/features/{1}.feat{2}".format(base_dir, shard, field)
        if not os.path.exists(feat_file_path):
            shards_size[shard] = 0
            shards_tf[shard] = 0
            shards_features[shard] = {}
            continue
        feat, size, shard_tf = read_feat_file(feat_file_path)
        shards_features[shard] = feat
        shards_size[shard] = size
        shards_tf[shard] = shard_tf

    for query_id, query in queries:
        res = gen_cori_lst(shards_features, query, shards_size)

        outfile_path = "{0}/{1}_{2}{3}.rank_cori".format(res_dir, query_id, args.method, field)
        outfile = open(outfile_path, 'w')
        for score, shard in res:
            outfile.write('{0} {1}\n'.format(shard, score))
        outfile.close()

main()
