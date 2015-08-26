#!/opt/python27/bin/python
import argparse
import os
import cent_kld


def read_feat_file(filepath):
    term2prob = {}
    shard_size = 0
    for line in open(filepath):
        t, df, sum_tf, sum_prob = line.split()
        t = t.strip()
        if t == '-1':
            shard_size = int(df)
            continue
        p = float(sum_prob) / shard_size
        term2prob[t] = p
    return term2prob, shard_size

parser = argparse.ArgumentParser()
parser.add_argument("partition_name")
parser.add_argument("int_query_file", type=argparse.FileType('r'), help="queries in int format (queryid:queryterms)")
parser.add_argument("--method", "-m", default="lm")
parser.add_argument("--miu", "-i", type=float, default=0.01)
parser.add_argument("--cent", "-c", default="sample")

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
for shard in shards:
    feat_file_path = "{0}/features/{1}.feat".format(base_dir, shard)
    if not os.path.exists(feat_file_path):
        shards_size[shard] = 0
        shards_features[shard] = {}
        continue
    cent, size = read_feat_file(feat_file_path)
    shards_features[shard] = cent
    shards_size[shard] = size

# get reference model for smoothing
ref = {}
ndocs = 0
for shard in shards:
    cent = shards_features[shard]
    size = shards_size[shard]
    ndocs += size
    for term in cent:
        ref[term] = ref.get(term, 0) + cent[term] * size
for term in ref:
    ref[term] /= ndocs


for query_id, query in queries:
    res = cent_kld.gen_lst(shards_features, ref, query, args.method, args.miu)

    outfile_path = "{0}/{1}.rank".format(res_dir, query_id)
    outfile = open(outfile_path, 'w')
    for score, shard in res:
        outfile.write('{0} {1}\n'.format(shard, score))
    outfile.close()

