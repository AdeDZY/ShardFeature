#!/opt/python27/bin/python
import os, sys
import string
import jobWriter

n_files = 0
n_jobs = 0

f = open('taily_{0}.job'.format(n_files),'w') # job name
for i in range(0, 533):
	# gengerate condor jobs	
	job = jobWriter.jobGenerator("./gen_shard_lst_taily.sh", "cachetest-new queries/aol-queries-4to6weeks-new/{0}.intQueries -i {0} -v 30".format(i), '/tmp/zhuyund_kmeans.log', 'taily.out', 'taily.err')
	f.write(job )
	n_jobs += 1
	if n_jobs % 50 == 0:
		f.close()
		n_files += 1
		f = open('taily_{0}.job'.format(n_files),'w') # job name
     
f.close()

