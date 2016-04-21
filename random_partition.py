#!/opt/python27/bin/python

import numpy as np
import os
import argparse
import random

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("extid_file")
    parser.add_argument("n_shards", type=int)
    parser.add_argument("output_dir")
    args = parser.parse_args()

    fs = [open("{0}/{1}".format(args.output_dir, i + 1), 'w') for i in range(args.n_shards)]
    with open(args.extid_file) as f:
        for line in f:
            s = random.randint(1, args.n_shards)
            fs[s - 1].write(line)
    for f in fs:
        f.close()
