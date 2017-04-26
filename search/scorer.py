#!/usr/bin/env python
# encoding: utf-8

import os
import pickle
import time
import bz2

from tornado.web import RequestHandler, Application
from tornado.ioloop import IOLoop

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer

from search.utils.tokenizer import StemTokenizer, SimpleTokenizer

class Scorer:
    def __init__(self, _tfidf, doc_reps, doc_invidx,
                 id2repid=None,
                 tokenizer=StemTokenizer(), simple_tokenizer=SimpleTokenizer()):
        self.tfidf = _tfidf
        self.doc_reps = doc_reps
        self.doc_invidx = doc_invidx
        self.ndocs = doc_reps.shape[0]
        self.tokenizer = tokenizer
        self.simple_tokenizer = simple_tokenizer
        self.id2repid = id2repid

    def tokenize(self, q, method="accurate"):
        tokens = []
        if method == "accurate":
            tokens = self.simple_tokenizer(q)
        elif method == "blur":
            tokens = self.tokenizer(q)

        return tokens

    def _get_docsid(self, q, method="accurate"):
        tokens = self.tokenize(q, method)
        docids = set()
        for token in tokens:
            if token in self.doc_invidx:
                docids = docids.union(self.doc_invidx[token][:20])

        return list(docids)

    def _id2repid(self, ids):
        return [self.id2repid[x] for x in ids]

    def _get_unbiased_scores(self, q_vec, docids):
        m_bonus = np.zeros([self.ndocs])
        ids_in_rep = self._id2repid(docids)
        trimmed_rep = self.doc_reps[ids_in_rep].toarray()
        m = trimmed_rep[:, 1:].dot(q_vec.T).reshape([trimmed_rep.shape[0]])
        return np.c_[trimmed_rep[:, 0], m]

    def scores(self, q):
        # get query tf-idf vector
        q_vec = self.tfidf.transform([q]).toarray()

        # accurate search
        docids = self._get_docsid(q)
        scores_acc = self._get_unbiased_scores(q_vec, docids)

        # blur search
        docids_blur = self._get_docsid(q, 'blur')

        # trim results in accurate search
        docids_blur = list(set(docids_blur).difference(docids))
        scores_blur = self._get_unbiased_scores(q_vec, docids_blur)

        # rescale socre in blur search
        scores_blur = np.c_[scores_blur[:, 0], scores_blur[:, 1] * 0.2]

        scores = np.r_[scores_acc, scores_blur]
        scores = [[int(x[0]), round(x[1], 2)] for x in scores]

        return sorted(scores, key=lambda x: x[1], reverse=True)

def make_scorer(tfidf_f, data_f):
    tfidf = pickle.load(open(tfidf_f, "rb"))
    data = pickle.load(bz2.open(data_f, "rb"))
    id2repid = data["id2repid"]
    return Scorer(tfidf, data["doc_rep"], data["doc_invidx"], id2repid=id2repid)
