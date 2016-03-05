#!/usr/bin/env python3
import sys

col = int(sys.argv[2])
with open(sys.argv[1], 'r') as f:
    for l in f:
        print(l.split('\t')[col])
