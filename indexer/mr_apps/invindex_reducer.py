#!/usr/bin/env python

import sys
import json

from itertools import groupby, tee
from operator import itemgetter

MAX_INVINDEX_LEN = 100

df = []
tf = {}

data = map(lambda x: x.strip().split('\t'), sys.stdin)
for k, g in groupby(data, itemgetter(0)):
    if k == '#D':
        print("#D\t%d" % sum([int(x[1]) for x in g]))
        continue

    terms = [x[1] for x in g]
    count = {}

    for t in terms:
        count[t] = count.get(t, 0) + 1

    for t in count:
        tf.setdefault(t, [])
        tf[t].append((int(k), count[t]))

for term in tf:
    tf[term].sort(key=itemgetter(1), reverse=True)
    df.append((term, len(tf[term]))) # append df
    print("%s\t%s" % (term, ",".join(map(lambda x: "%d:%d" % (x[0], x[1]),
                                         tf[term][:MAX_INVINDEX_LEN]))))

sorted(df, key=itemgetter(0))
print("#DF\t%s" % json.dumps(df))
