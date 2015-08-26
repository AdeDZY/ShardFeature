#!/opt/python27/bin/python
import argparse
import numpy as np


def score_lm(qterms, feat, ref_dv, miu):
    s = []
    for token in qterms:
        pref = ref_dv.get(token, 0.000000000000001)
        if token in feat:
            pcent = feat[token][2]
        else:
            pcent = 0
        pcent = (1 - miu) * pcent + miu * pref
        contri = np.log(pcent)
        s.append(contri)
    return sum(s)


def score_indri(qterms, feat, ref, miu, lamb, shard_tf):
    s = []
    for token in qterms:
        pref = ref.get(token, 0.000000000000001)
        if token in feat:
            tf = feat[token][1]
        else:
            tf = 0
        p_smoothed = (1 - miu) * (tf + lamb * pref)/(shard_tf + lamb) + miu * pref
        s.append(np.log(p_smoothed))
    return sum(s)


def score_kld(qterms, cent, ref, miu):
    s = []
    pdoc = 1.0/(len(qterms) + 1)
    for token in qterms:
        pref = ref.get(token, 0.5)
        pcent = (1 - miu) * cent.get(token, 0) + miu * pref
        contri = pcent * np.log(pdoc / pref)
        contri += pdoc * np.log(pcent / pref)
        s.append(contri)
        # s.append( np.log(pcent) * np.log(pcent / ref[token]))
    return sum(s)


def gen_lst(shards_features, ref_dv, ref, query, method, miu, lamb, shards_tf):
    if lamb < 0:
        lamb = 25205000.0 / len(shards_features)
    qterms = query.split()

    res = []
    for shard in shards_features:
        feat = shards_features[shard]

        if method == 'kld':
            s = score_kld(qterms, feat, ref_dv, miu)
        if method == "lm":
            s = score_lm(qterms, feat, ref, miu)
        if method == "indri":
            s = score_indri(qterms, feat, ref_dv, miu, lamb, shards_tf[shard])
        res.append((s, shard))

    sorted_res = sorted(res, reverse=True)
    return sorted_res

