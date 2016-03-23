#!/opt/python27/bin/python
import argparse
import os
import cent_kld


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("partition_name")
    args = parser.parse_args()

    base_dir = "/bos/usr0/zhuyund/partition/ShardFeature/output/" + args.partition_name + "/features/"
    shard_file = base_dir + "/shards"
    shards = []
    for line in open(shard_file):
        shards.append(line.strip())
    fields = ["title", "url", "inlink"]
    for shard in shards:
        feat_file_path = "{0}/features/{1}.feat_fielded".format(base_dir, shard)
        fdx = -1
        fout = open('tmp.f', 'w')
        with open(feat_file_path) as f:
            for line in f:
                if line.startswith('-1'):
                    fout.close()
                    fdx += 1
                    fout = open("{0}/features/{1}.feat_{2}".format(base_dir, shard, fields[fdx]))
                fout.write(line)
        fout.close()





