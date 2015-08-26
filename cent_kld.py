#!/opt/python27/bin/python
import argparse
import numpy as np


def score_lm(qterms, cent, ref, miu):
	s = []
	for token in qterms:
		pref = ref.get(token, 0.5)
		pcent = (1 - miu) * cent.get(token, 0)+ miu * pref 
		contri = np.log(pcent)
		s.append(contri)
	return sum(s)

def score_kld(qterms, cent, ref, miu):
	s = []
	pdoc = 1.0/(len(qterms) + 1)
	for token in qterms:
		pref = ref.get(token, 0.5)
		pcent = (1 - miu) * cent.get(token, 0)+ miu * pref 
		contri = pcent * np.log(pdoc/( pref ))
		contri += pdoc * np.log(pcent/(pref))
		s.append(contri)
		#s.append( np.log(pcent) * np.log(pcent / ref[token]))
	return sum(s)
	

def gen_lst(centroids, ref, nCentroids, query, method, miu):
	qterms = query.split()
	print qterms

	res = []
	for p in range(0, nCentroids):
		cent = centroids[p] 
		
		if method == 'kld':
			s = score_kld(qterms, cent, ref, miu)	
		else:
			s = score_lm(qterms, cent, ref, miu)
		res.append((s, p + 1))

	sorted_res = sorted(res, reverse=True)
	return sorted_res	


	
			
		
