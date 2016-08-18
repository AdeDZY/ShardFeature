#!/opt/python27/bin/python
import argparse
import os
from scipy.stats import gamma


class ShardTermFeat:
    def __init__(self):
        self.df = 0     # number of documents in the shard that contain the term
        self.e = 0      # eq3, expectation of the term's log likelihood
        self.sqr_e = 0  # eq3, expectation of (log_likelihood)**2
        self.var = 0    # eq4, variance of log likelihood
        self.min = 0    # min value of log likelihood in this shard


class ShardFeat:
    def __init__(self):
        self.e = 0      # expectation of the query's log likelihood in the shard
        self.var_e = 0
        self.any = 0    # eq10
        self.all = 0    # eq10
        self.size = 0   # this shard's size
        self.k = 0      # gamma shape parameter
        self.theta = 0  # gamma scale parameter


def read_feat_file(filepath):
    """
    Read the raw feature file, and return term_features for a shard
    :param filepath: the feature file
    :return:
        term2feat={}, key: term (string), value: ShardTermFeat
        shard_size: int
    """
    term2feat = {}
    shard_size = 0
    for line in open(filepath):
        t, df, sum_tf, sum_prob, sum_logprob, sum_sqr_logprob, min_logprob = line.split()
        t = t.strip()
        if '-1' in t:
            shard_size = int(df) 
            continue
        df = int(df)
        sum_logprob = float(sum_logprob)
        sum_sqr_logprob = float(sum_sqr_logprob)
        min_logprob = float(min_logprob)
        feat = ShardTermFeat()
        feat.df = int(df)
        feat.e = sum_logprob / df
        feat.sqr_e = sum_sqr_logprob / df
        feat.var = feat.sqr_e - feat.e**2
        feat.min = min_logprob
        term2feat[t] = feat
    return term2feat, shard_size


def get_any(shard, qterms, shard_term_features, shard_size):
    """
    Compute eq10, # of documents in the shard that contain at least one query term
    :param shard: int, the shard being processed
    :param qterms: a list of terms (strings)
    :param shard_term_features: shard_term_features[shard][term]=ShardTermFeat(). shard0 is the whole collection
    :param shard_size: int, the size of shard being processed
    :return: Any value of this shard, float
    """
    d = float(shard_size)
    # first compute the prob for a document in this shard to contain none of the query terms
    tmp = 1
    for t in qterms:
        t = t.strip()
        if t in shard_term_features[shard]:
            cdf = shard_term_features[shard][t].df
        else:
            cdf = 0
        tmp *= 1 - cdf / d

    # than compute any
    any = (1 - tmp) * d
    return any


def get_all(any, shard, shard_term_features, qterms):
    """
    Compute eq10, # of documents in the shard that contain at all query terms
    :param any:  any value for this shard, eq10
    :param shard: int, the shard being processed
    :param shard_term_features: shard_term_features[shard][term]=ShardTermFeat(). shard0 is the whole collection
    :param qterms: a list of terms (strings)
    :return: All value of the shard, float
    """
    tmp = 1
    for t in qterms:
        if t in shard_term_features[shard]:
            cdf = shard_term_features[shard][t].df
        else:
            cdf = 0
        tmp *= cdf/any
    all = tmp * any
    return all


def get_shift(qterms, shard_term_features):
    """
    The shift value in eq5, sum of min logprob of query terms
    :param qterms: a list of terms (strings)
    :param shard_term_features: shard_term_features[shard][term]=ShardTermFeat(). shard0 is the whole collection
    :return: float
    """
    shift = 0
    for t in qterms:
        mint = float("inf")
        for s in shard_term_features:
            if t in shard_term_features[s]:
                if mint > shard_term_features[s][t].min:
                    mint = shard_term_features[s][t].min
        if mint != float("inf"):
            shift += mint
    return shift


def get_shard_e(shard, shard_term_features, qterms, shift):
    """
    eq5
    :param shard: the shard being processed, int
    :param shard_term_features: shard_term_features[shard][term]=ShardTermFeat(). shard0 is the whole collection
    :param qterms:  a list of terms (strings)
    :param shift: float, the shift value
    :return: float
    """
    e = 0
    for t in qterms:
        if t in shard_term_features[shard]:
            e += shard_term_features[shard][t].e
    return e + shift


def get_shard_var(shard, shard_term_features, qterms):
    """
    eq6
    :param shard:
    :param shard_term_features:
    :param qterms:
    :return:
    """
    var = 0
    for t in qterms:
        if t in shard_term_features[shard]:
            var += shard_term_features[shard][t].var
    return var


def prepare_shard_features(shard_term_features, qterms, shard_features):
    """
    prepare any, all, e, var, k and theta for each shard
    :param shard_term_features: shard_term_features[shard][term]=ShardTermFeat(). shard0 is the whole collection
    :param qterms:  a list of terms (strings)
    :param shard_features: shard_features[shard] = ShardFeature. shard0 is the whole collection.
    :return: None
    """
    # compute shift
    shift = get_shift(qterms, shard_term_features)
    n_shards = len(shard_term_features)

    # define k and theta
    f_k = lambda e, var: e ** 2 / var
    f_theta = lambda e, var: var / e

    # compute any, all, expectation, var, k and theta
    for s in range(n_shards + 1):
        shard_features[s].any = get_any(s, qterms, shard_term_features, shard_features[s].size)
        shard_features[s].all = get_all(shard_features[s].any, s, shard_term_features, qterms)
        shard_features[s].e = get_shard_e(s, shard_term_features, qterms, shift)
        shard_term_features[s].var = get_shard_var(s, shard_term_features, qterms)
        shard_term_features[s].k = f_k(shard_features[s].e, shard_term_features[s].var)
        shard_term_features[s].theta = f_theta(shard_features[s].e, shard_term_features[s].var)


def rank_taily(shard_term_features, qterms, n_c, shard_features):
    """
    Get taily scores (ni) for each shard in terms of the query.
    :param shard_term_features: shard_term_features[shard][term]=ShardTermFeat(). shard0 is the whole collection
    :param qterms: a list of terms (strings)
    :param n_c: taily parameter
    :param shard_features: shard_features[shard] = ShardFeature. shard0 is the whole collection.
    :return: a ranked list of (n_i, i). i is the shard index starting from 1
    """

    # get all, k, theta for each shard
    prepare_shard_features(shard_term_features, qterms, shard_features)

    # compute s_c
    p_c = n_c/shard_features[0].all
    s_c = gamma.ppf(p_c, shard_features[0].k, scale=shard_features[0].theta)

    # compute pi and ni
    n_shards = len(shard_features) - 1  # first is all
    taily_scores = []

    # normalizer in eq12
    normalizer = 0

    # compute ni for each shard
    for s in range(1, n_shards + 1):
        # eq9, the cdf(survival function)
        pi = gamma.sf(s_c, shard_features[s].k, scale=shard_features[s].theta)
        # compute normalizer
        normalizer += pi * shard_features[s].all
        # eq12, without normalziation
        ni = shard_features[s].all * pi
        taily_scores.append(ni)

    # normalize
    for i in range(len(taily_scores)):
        taily_scores[i] *= n_c/normalizer

    # sort in reverse order and return
    return sorted([(ni, s + 1) for s, ni in enumerate(taily_scores)], reverse=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("partition_name")
    parser.add_argument("int_query_file", type=argparse.FileType('r'), help="queries in int format (queryid:queryterms)")
    parser.add_argument("--n_c", "-n", type=float, default=400)
    #parser.add_argument("--v", "-v", type=float, default=50)
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
        shards.append(int(line.strip()))
    n_shards = len(shards)

    # read in all feature files
    shard_term_features = [{} for i in range(n_shards + 1)]
    shard_features = [ShardFeat() for i in range(n_shards + 1)]

    for shard in shards:
        feat_file_path = "{0}/features/{1}.feat_taily".format(base_dir, shard)
        if not os.path.exists(feat_file_path):
            continue
        feats, size = read_feat_file(feat_file_path)
        shard_term_features[shard] = feats
        shard_features[shard].size = size

    # compute the features for the whole collections, indexed as shard 0
    shard_term_features[0] = {}
    for feats in shard_term_features.values():
        for t in feats:
            df, e, sqr_e, var = feats[t]
            if t not in shard_term_features[0]:
                shard_term_features[0][t] = ShardTermFeat()
            shard_term_features[0][t].df += df
            shard_term_features[0][t].e += e * df
            shard_term_features[0][t].sqr_e += sqr_e * df
    for t in shard_term_features[0]:
        shard_term_features[0][t].e /= shard_term_features[0][t].df
        shard_term_features[0][t].sqr_e /= shard_term_features[0][t].df
        shard_term_features[0][t].var = shard_term_features[0][t].sqr_e - shard_term_features[0][t].e ** 2
    shard_features[0].size = sum([s.size for s in shard_features[1:]])

    for query_id, query in queries:
        qterms = [t.strip() for t in queries.split()]
        res = rank_taily(shard_term_features, qterms, args.n_c, shard_features)

        outfile_path = "{0}/{1}_{2}{3}.rank_taily".format(res_dir, query_id, args.method)
        outfile = open(outfile_path, 'w')
        for score, shard in res:
            outfile.write('{0} {1}\n'.format(shard, score))
        outfile.close()

if __name__ == '__main__':
    main()
