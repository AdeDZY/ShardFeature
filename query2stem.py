#!/opt/python27/bin/python

__author__ = 'zhuyund'

import argparse
import os, sys
import jobWriter
from os import listdir
from os.path import isfile, join

parser = argparse.ArgumentParser()
parser.add_argument("query_file", type=argparse.FileType('r'))
parser.add_argument("query_term2int_file", type=argparse.FileType('r'))
parser.add_argument("start_query_id", type=int, help="gov2:701; cwb:1")
args = parser.parse_args()

dic = {}
for line in args.query_term2int_file:
    term, term_id = line.split()[0:2]
    dic[term] = term_id

qid = args.start_query_id
for line in args.query_file:
    terms = line.split()
    term_ids = [dic[term] for term in terms if term in dic]
    s = ' '.join(term_ids)
    print  s
    qid += 1
    #if qid == 803:
    #    qid += 1

