#!/opt/python27/bin/python
import argparse
import sys, os
import cent_kld


def read_prob_file(filepath, terms):
	term2prob = {}
	nline = 0
	nfound = 0
	for line in open(filepath):
		nline += 1
		if ',' not in line:
			break
		t, pstr = line.split(',')
		t = t.strip()
		if t in terms:
			p = float(pstr)
			term2prob[t.strip()] = p 		
			nfound += 1
			if nfound == len(terms):
				break
	return term2prob

parser = argparse.ArgumentParser()
parser.add_argument("centroidsDir")
parser.add_argument("reference")
parser.add_argument("queryFile", type=argparse.FileType('r'))
parser.add_argument("shardlim", type=int)
parser.add_argument("resDir")
parser.add_argument("nCentroids", type=int)
parser.add_argument("--method","-m", default="lm")
parser.add_argument("--miu","-i", type=float, default=0.2)
parser.add_argument("--cent","-c", default="sample")

args = parser.parse_args()

if not os.path.exists(args.resDir):
	os.makedirs(args.resDir)

outfile = open("{0}/all.shardlist".format(args.resDir), 'w')

qterms = {}
queries = []
for query in args.queryFile:
	query = query.strip()
	queries.append(query)	
	for t in query.split():
		qterms[t] = 1

ref = read_prob_file(args.reference, qterms)
centroids = []
for i in range(1, args.nCentroids + 1):
	cent = {}
	if args.cent == "sample":
		cent = read_prob_file("{0}/{1}".format(args.centroidsDir, i), qterms)
	else:	
		cent = read_prob_file("{0}/{1}.centroid".format(args.centroidsDir, i), qterms)
	centroids.append(cent)

i = 0
for query in queries:
	i += 1
	res = cent_kld.gen_lst(centroids, ref, args.nCentroids, query, args.method, args.miu)
	outfile.write(str(i-1) + ' ')
	for s, q in res[0: args.shardlim]:
		outfile.write('{0} '.format(q))	
	outfile.write('\n')

