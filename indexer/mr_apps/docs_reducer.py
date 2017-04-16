#!/usr/bin/env python

import sys
import pickle
import json
import numpy as np
import nltk.data
import os

from functools import reduce
from operator import itemgetter
from itertools import count
from scipy.sparse import csr_matrix, vstack, hstack

from indexer.dist_tfidf import DistTFIDFVectorizer
from search.manifest import TITLE_BONUS


idf_file = os.environ.get("IDF_FILE", default="indexer/idf_jobs/0.out")
print(idf_file)
vec = DistTFIDFVectorizer(idf_file)

sent_detector = nltk.data.load('tokenizers/punkt/english.pickle')

'''
{
    "metadata" : self._metadata[i],
    "sents": sents,
    "sents_rep" : sents_rep,
}
'''

docs, doc_reps, id2repid = {}, [], {}

doc_batch = []
title_batch = []
id_batch = []

BATCH_SIZE = 1000

data = map(lambda x: x.strip().split('\t')[1], sys.stdin)
for doc, i in zip(data, count()):
    doc_data = json.loads(doc)
    doc_id = int(doc_data["metadata"]["doc_id"])
    sents = sent_detector.tokenize(doc_data["doc"])
    docs[doc_id] = {
        "metadata" : doc_data["metadata"],
        "sents" : sents,
    }

    doc_batch.append(doc_data["doc"])
    title_batch.append(doc_data["metadata"]["title"])
    id_batch.append([doc_id])

    if i % BATCH_SIZE == BATCH_SIZE - 1:
        dr = vec.transform(doc_batch)
        tr = vec.transform(title_batch)
        dr = dr + tr * TITLE_BONUS
        doc_rep = hstack((np.array(id_batch), dr))
        doc_reps.append(doc_rep)
        doc_batch, title_batch, id_batch = [], [], []
    id2repid[doc_id] = i

if len(doc_batch) > 0:
    dr = vec.transform(doc_batch)
    tr = vec.transform(title_batch)
    dr = dr + tr * TITLE_BONUS
    doc_rep = hstack((np.array(id_batch), dr))
    doc_reps.append(doc_rep)
    doc_batch, title_batch, id_batch = [], [], []

pickle.dump({
    "docs" : docs,
    "doc_reps" : vstack(doc_reps, 'csr'),
    "id2repid" : id2repid
}, sys.stdout.buffer)

