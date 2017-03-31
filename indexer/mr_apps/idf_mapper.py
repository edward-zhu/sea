#!/usr/bin/env python

import sys

for line in sys.stdin:
    term, doc_tfs = line.strip().split('\t')
    if term[0] == '#':
        print(line.strip())


