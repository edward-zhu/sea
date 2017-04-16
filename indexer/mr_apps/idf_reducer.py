#!/usr/bin/env python
#pylint:disable=C0103

import sys
import pickle
import json
from functools import reduce
from itertools import groupby, count
from operator import itemgetter, contains

import numpy as np
from scipy.sparse import csr_matrix

MAX_DF = 0.9995
MIN_DF = 0.0005

def check_and_get_union(_iter, key):
    _t, _g = next(_iter)
    if _t != key:
        raise KeyError("not what you want: %s" % key)

    ret = []
    for x in _g:
        ret.extend(json.loads(x[1]))

    return ret

def check_and_get_sum(_iter, key):
    _t, _g = next(_iter)
    if _t != key:
        raise KeyError("not what you want: %s" % key)
    return sum([int(i[1]) for i in _g])

data = map(lambda x: x.strip().split('\t'), sys.stdin)
giter = groupby(data, itemgetter(0))

# get document count D
n_docs = check_and_get_sum(giter, "#D")
# get DF
raw_df = sorted(check_and_get_union(giter, "#DF"), key=itemgetter(0))
accumulated_df = [(x, sum(map(itemgetter(1), y))) for x, y in groupby(raw_df, itemgetter(0))]

# truncated outlier term
_min, _max = MIN_DF * n_docs, MAX_DF * n_docs
norm_df = filter(lambda x: (x[1] < _max and x[1] > _min), accumulated_df)

_t, _df = zip(*norm_df)
terms = dict(zip(_t, count()))

# get normed term count T
n_terms = len(terms)

# assign df
df = np.zeros(n_terms)
df[:] = list(_df)

# sklearn recipe
idf = np.log((n_docs + 1) / (df + 1)) + 1

pickle.dump({
    "idf" : idf,
    "dic" : terms
}, sys.stdout.buffer)
