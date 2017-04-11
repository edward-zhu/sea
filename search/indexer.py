#!/usr/bin/env python
# encoding: utf-8

from sklearn.feature_extraction.text import TfidfVectorizer
import nltk.data
import numpy as np
import pickle
from copy import deepcopy
import os
import hashlib
import scipy.sparse

from parser import MediaWikiParser
from utils.tokenizer import StemTokenizer
import manifest

class Indexer:
    def __init__(self, docs, metadata, title_bonus, tokenizer=StemTokenizer()):
        self._docs = docs
        self._metadata = metadata
        self._titles = [x["title"] for x in metadata]
        self._vectorizer = None
        self._doc_reps = None
        self._vocabulary = None
        self._tokenizer = tokenizer
        self._doc_invidx = None
        self._title_bonus = title_bonus
        self._sent_detector = nltk.data.load('tokenizers/punkt/english.pickle')

    def vectorizer(self):
        if self._vectorizer is None:
            self._vectorizer = TfidfVectorizer(stop_words="english", max_df=0.998, min_df=0.002,
                                        tokenizer=self._tokenizer, sublinear_tf=True, norm=None)
            dr = self._vectorizer.fit_transform(self._docs);
            # add title bonus
            tr = self._vectorizer.transform(self._titles)
            dr = dr + tr * self._title_bonus
            ids = scipy.sparse.csr_matrix(np.arange(0, len(self._docs), 1))
            self._doc_reps = scipy.sparse.hstack((ids.T, dr), "csr")

        return self._vectorizer

    def doc_reps(self):
        if self._doc_reps is None:
            self.vectorizer()

        return self._doc_reps

    def vocabulary(self):
        if self._vocabulary is None:
            self._vocabulary = self.vectorizer().vocabulary_
        return self._vocabulary

    def inverted_index(self):
        if self._doc_invidx is not None:
            return self._doc_invidx

        inverted_index = {}

        for term in self.vocabulary():
            inverted_index[term] = []

        for i, doc in enumerate(self._docs):
            terms = set([x.lower() for x in self._tokenizer(doc)])
            for term in terms:
                if term in inverted_index:
                    inverted_index[term].append(i)

        self._doc_invidx = inverted_index
        return inverted_index

    def gen_sentence_rep(self, doc):
        sents = self._sent_detector.tokenize(doc)
        return sents, self.vectorizer().transform(sents)

    def save_docs(self, prefix):
        partitioned = []
        nsrv = manifest.N_DOC_SRV
        for i in range(0, nsrv):
            partitioned.append({})

        for i in range(0, len(self._docs)):
            sents, sents_rep = self.gen_sentence_rep(self._docs[i])
            partitioned[i % nsrv][i] = {
                "metadata" : self._metadata[i],
                "sents": sents,
                "sents_rep" : sents_rep,
            }
            #print('docs: %d/%d' % (i, len(self._docs)))

        for i in range(0, nsrv):
            filename = "%s_%d.pkl" % (prefix, i)
            with open(filename, 'wb') as f:
                pickle.dump(partitioned[i], f, pickle.HIGHEST_PROTOCOL)

    def save_indexes_by_term(self, prefix):
        inverted_index = self.inverted_index()
        doc_reps = self.doc_reps()
        partitioned = []
        nsrv = manifest.N_INDEX_SRV
        for i in range(0, nsrv):
            partitioned.append({
                "doc_rep" : None,
                "doc_invidx" : {}
            })

        for term in inverted_index:
            bucket = int(hashlib.sha1(term.encode()).hexdigest(), 16) % nsrv
            partitioned[bucket]["doc_invidx"][term] = inverted_index[term]

        for i in range(0, nsrv):
            _filter = np.arange(i, doc_reps.shape[0], nsrv)
            partitioned[i]["doc_rep"] = doc_reps[_filter]

        for i in range(0, nsrv):
            filename = "%s_%d.pkl" % (prefix, i)
            with open(filename, 'wb') as f:
                pickle.dump(partitioned[i], f, pickle.HIGHEST_PROTOCOL)

    def save_indexes_by_doc(self, prefix):
        inverted_index= self.inverted_index()
        doc_reps = self.doc_reps()
        index_dic = {
            "doc_rep" : {},
            "doc_invidx": {},
        }

        for term in inverted_index:
            index_dic["doc_invidx"][term] = []

        nsrv = manifest.N_INDEX_SRV
        partitioned = []
        for i in range(0, nsrv):
            partitioned.append(deepcopy(index_dic))

        for term, docs in inverted_index.items():
            for doc in docs:
                partitioned[doc % nsrv]["doc_invidx"][term].append(doc);

        for i in range(0, nsrv):
            _filter = np.arange(i, doc_reps.shape[0], nsrv)
            partitioned[i]["doc_rep"] = doc_reps[_filter]
            filename = "%s_%d.pkl" % (prefix, i)
            with open(filename, 'wb') as f:
                pickle.dump(partitioned[i], f, pickle.HIGHEST_PROTOCOL)

    def save_indexes(self, prefix, method='doc'):
        if method == 'doc':
            self.save_indexes_by_doc(prefix)
        elif method == 'term':
            self.save_indexes_by_term(prefix)
        else:
            raise KeyError("No such save method.")

    def save_vectorizer(self, filename):
        with open(filename, 'wb') as f:
            pickle.dump(self.vectorizer(), f, pickle.HIGHEST_PROTOCOL)
"""
if __name__ == "__main__":
    print("parsing docs from %s.." % INPUT_FILE)
    metadata, docs = parse_doc(INPUT_FILE)
    print("parsing finished, got %d docs." % len(docs))
    print("generating idf and vocabulary..")
    vectorizer, doc_reps = get_vectorizer(docs)
    tokenizer = vectorizer.build_tokenizer()
    vocabulary = vectorizer.vocabulary_
    print("vocabulary contains %d words." % len(vocabulary))
    print("generating inversed indexes..")
    inverted_index, title_invidx = gen_inverted_index(docs, metadata, vocabulary, tokenizer)
    print("output path: %s" % os.path.abspath(OUTPUT_PATH))
    print("saving docs...")
    save_docs("docs", docs, metadata)
    print("saving inverted indexes...")
    save_indexes_by_doc("indexes", inverted_index, title_invidx, doc_reps)
    print("saving vectorizer..")
    save_vectorizer("tfidf.pkl", vectorizer)
    print("done.")
    with open(os.path.join(OUTPUT_PATH, "doc_reps.pkl"), "wb") as f:
        pickle.dump(doc_reps, f, pickle.HIGHEST_PROTOCOL)
"""

if __name__ == '__main__':
    input_file = "data/enwiki_1.xml"
    parser = MediaWikiParser()
    docs, metadata = parser.parse(input_file)
    indexer = Indexer(docs, metadata, manifest.TITLE_BONUS)
    print('saving docs...')
    indexer.save_docs(os.path.join(manifest.DATA_DIR, "docs"))
    print('saving indexes...')
    indexer.save_indexes(os.path.join(manifest.DATA_DIR, "indexes"))
    print('saving model...')
    indexer.save_vectorizer(os.path.join(manifest.DATA_DIR, "tfidf.pkl"))
    print('done')
