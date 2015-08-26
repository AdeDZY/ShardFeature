#!/opt/python27/bin/python
import argparse
import os
import cent_kld


def read_feat_file(filepath):
    term2feat = {}
    shard_size = 0
    shard_tf = 0
    for line in open(filepath):
        t, df, sum_tf, sum_prob = line.split()
        t = t.strip()
        if t == '-1':
            shard_size = int(df)
            shard_tf = int(sum_tf)
            continue
        p = float(sum_prob) / shard_size
        term2feat[t] = (int(df), int(sum_tf), p)
    return term2feat, shard_size, shard_tf

parser = argparse.ArgumentParser()
parser.add_argument("partition_name")
parser.add_argument("int_query_file", type=argparse.FileType('r'), help="queries in int format (queryid:queryterms)")
parser.add_argument("--method", "-m", default="lm")
parser.add_argument("--miu", "-i", type=float, default=0.0001)
parser.add_argument("--lamb", "-l", type=float, default=500)

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
for shard in shards:
    feat_file_path = "{0}/features/{1}.feat".format(base_dir, shard)
    if not os.path.exists(feat_file_path):
        shards_size[shard] = 0
        shards_tf[shard] = 0
        shards_features[shard] = {}
        continue
    feat, size, shard_tf = read_feat_file(feat_file_path)
    shards_features[shard] = feat
    shards_size[shard] = size
    shards_tf[shard] = shard_tf

# get reference model for smoothing
ref_dv = {} # average of doc vectors
ref = {} # tf_in_shard / total_tf_of_shard
ndocs = 0
nterms = 0
for shard in shards:
    feat = shards_features[shard]
    size = shards_size[shard]
    shard_tf = shards_tf[shard]
    ndocs += size
    nterms += shard_tf
    for term in feat:
        df, sum_tf, sum_prob = feat[term]
        ref_dv[term] = ref_dv.get(term, 0.0) + sum_prob * size
        ref[term] = ref.get(term, 0.0) + float(sum_tf)
for term in ref_dv:
    ref_dv[term] /= ndocs
    ref[term] /= nterms


for query_id, query in queries:
    res = cent_kld.gen_lst(shards_features, ref_dv, ref, query, args.method, args.miu, args.lamb, shards_tf, shards_size)

    outfile_path = "{0}/{1}_{2}.rank".format(res_dir, query_id, args.method)
    outfile = open(outfile_path, 'w')
    for score, shard in res:
        outfile.write('{0} {1}\n'.format(shard, score))
    outfile.close()

