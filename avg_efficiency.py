#!/opt/python27/bin/python

import numpy as np
import os
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("file_path")
    args = parser.parse_args()

    ext = 0
    res = 0
    lat = 0
    n = 0
    with open(args.file_path) as f:
        for line in f:
            n += 1
            name, cext, cres, clat = line.split()
            ext += float(cext)
            res += float(cres)
            lat += float(clat)
    ext /= n
    res /= n
    lat /= n
    print ext, res, lat