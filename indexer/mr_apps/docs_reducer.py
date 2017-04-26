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
from indexer.manifest import TITLE_BONUS

def get_doc_rep_batch(doc_batch, title_batch, title_len_batch, id_batch):
    dr = vec.transform(doc_batch)
    tr = vec.transform(title_batch)
    tl = (10 / np.array(title_len_batch)) ** 2
    dr = dr + (tr * TITLE_BONUS).multiply(tl.reshape(-1, 1))
    doc_rep = hstack((np.array(id_batch), dr))
    return doc_rep

idf_file = os.environ.get("IDF_FILE", default="indexer/idf_jobs/0.out")
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
title_len_batch = []
id_batch = []

BATCH_SIZE = 2000

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
    title_len_batch.append(len(doc_data["metadata"]["title"].split(" ")))
    id_batch.append([doc_id])

    if i % BATCH_SIZE == BATCH_SIZE - 1:
        doc_reps.append(get_doc_rep_batch(doc_batch, title_batch,
                                          title_len_batch, id_batch))
        doc_batch, title_batch, title_len_batch, id_batch = [], [], [], []
    id2repid[doc_id] = i

if len(doc_batch) > 0:
    doc_reps.append(get_doc_rep_batch(doc_batch, title_batch,
                                      title_len_batch, id_batch))

pickle.dump({
    "docs" : docs,
    "doc_reps" : vstack(doc_reps, 'csr'),
    "id2repid" : id2repid
}, sys.stdout.buffer)
