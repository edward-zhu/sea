#!/usr/bin/env python
# encoding: utf-8

import os
import pickle
import time
import bz2
import logging

from tornado.web import RequestHandler, Application
from tornado.ioloop import IOLoop

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer

from search.utils.tokenizer import StemTokenizer, SimpleTokenizer

logger = logging.getLogger(__name__)

class Scorer:
    def __init__(self, _tfidf, doc_reps, doc_invidx,
                 max_q_doc,
                 id2repid=None,
                 tokenizer=StemTokenizer(), simple_tokenizer=SimpleTokenizer()):
        self.tfidf = _tfidf
        self.doc_reps = doc_reps
        self.doc_invidx = doc_invidx
        self.ndocs = doc_reps.shape[0]
        self.tokenizer = tokenizer
        self.max_q_doc = max_q_doc
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
                if len(docids) == 0:
                    docids = set(self.doc_invidx[token])
                else:
                    docids = docids.intersection(self.doc_invidx[token])

        return list(docids)[:self.max_q_doc]

    def _id2repid(self, ids):
        return [self.id2repid[x] for x in ids]

    def _get_unbiased_scores(self, q_vec, docids):
        m_bonus = np.zeros([self.ndocs])
        t = time.time()
        ids_in_rep = self._id2repid(docids)
        logger.debug("map time %.4f", time.time() - t)

        t = time.time()
        trimmed_rep = self.doc_reps[ids_in_rep].toarray()

        logger.debug("trim time %.4f", time.time() - t)
        t = time.time()

        q_vec = np.c_[0, q_vec]
        m = trimmed_rep.dot(q_vec.T).reshape([trimmed_rep.shape[0]])

        logger.debug("dot time %.4f", time.time() - t)
        return np.c_[trimmed_rep[:, 0], m]

    def scores(self, q):
        # get query tf-idf vector
        q_vec = self.tfidf.transform([q]).toarray()

        # accurate search
        docids = self._get_docsid(q)

        scores_acc = self._get_unbiased_scores(q_vec, docids)

        docids_blur = []

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

def make_scorer(tfidf_f, data_f, max_q_doc):
    tfidf = pickle.load(open(tfidf_f, "rb"))
    data = pickle.load(bz2.open(data_f, "rb"))
    id2repid = data["id2repid"]
    return Scorer(tfidf, data["doc_rep"], data["doc_invidx"], max_q_doc, id2repid=id2repid)
