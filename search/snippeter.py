#!/usr/bin/env python
# encoding: utf-8

import pickle
import cgi
import re
import time
import numpy as np

from functools import reduce

from nltk.tokenize import RegexpTokenizer

from search.utils.tokenizer import SimpleTokenizer, StemTokenizer, SideEffectTokenizer

class Snippeter:
    def __init__(self, tfidf, docs, snippet_len, simple_tokenize=SimpleTokenizer()):
        self._tfidf = tfidf
        self._docs = docs
        self._vocabulary = tfidf.vocabulary_
        self._simple_tokenize = simple_tokenize
        self._rt = RegexpTokenizer(r'\s+', gaps=True)
        self._tt = tfidf.build_tokenizer()
        self._st = StemTokenizer()
        self._set = SideEffectTokenizer(True)
        self.snippet_len = snippet_len
        self._sent_reps = {}

    def _get_sent_reps(self, docid):
        if docid in self._sent_reps:
            return self._sent_reps[docid]

        reps = self._tfidf.transform(self._docs[docid]["sents"][:50])
        self._sent_reps[docid] = reps
        return reps

    def _get_accuate_termids(self, q):
        return [self._vocabulary[x] for x in self._simple_tokenize(q) if x in self._vocabulary]

    def _expand(self, sents, sentid, blur_q):
        expanded = []
        l = 0
        curid = sentid
        first_pos = 0
        while l < self.snippet_len and curid < len(sents):
            for t in self._rt.tokenize(sents[curid]):
                if t in blur_q:
                    first_pos = l
                expanded.append(t)
                l = l + 1
            curid += 1
        if len(expanded) > self.snippet_len:
            if curid > sentid + 1:
                expanded = expanded[: self.snippet_len]
            else:
                left = first_pos
                right = len(expanded) - first_pos
                half = int(self.snippet_len / 2)
                if right < half:
                    expanded = expanded[len(expanded) - half * 2:]
                elif left < half:
                    expanded = expanded[:half * 2]
                else:
                    expanded = expanded[first_pos - half: first_pos + half]

                expanded.insert(0, "... ")

        return " ".join(expanded) + " ..."

    def _highlight(self, snippet, q):
        _, tok_poses = self._set(snippet, True)
        poses = reduce(lambda x, y: x + tok_poses.get(y, []), set(q), [])
        poses.sort(key=lambda x: x[0])

        '''
        merged = []
        for pos in poses:
            if len(merged) > 0 and merged[-1][1] > pos[0]:
                merged[-1][1] = pos[1]
            else:
                merged.append(pos)
        '''

        merged = poses

        start = 0
        highlighted = []

        for pos in merged:
            highlighted.append(snippet[start: pos[0]])
            highlighted.append("<strong>")
            highlighted.append(snippet[pos[0]: pos[1]])
            highlighted.append("</strong> ")
            start = pos[1] + 1
        highlighted.append(snippet[start:])

        return "".join(highlighted)

    def _highlight_trival(self, snippet, q):
        pat = r"(" + r"|".join(q) + r")\w*"

        highlighted = re.sub(pat, lambda x: "<strong>" + x.group(0) + "</strong>",
                             snippet, flags=re.I)

        return highlighted

    def _gen_snippet(self, docid, q_rep, blur_q):
        sents = self._docs[docid]["sents"]
        sents_rep = self._get_sent_reps(docid)

        scores = sents_rep.dot(q_rep.T).toarray()
        scores[0] = scores[0] * 3
        max_id = np.argmax(scores)

        expanded = self._expand(sents, max_id, blur_q)
        highlighted = self._highlight(cgi.escape(expanded), blur_q)
        return highlighted

    def snippet_batch(self, docids, q):
        # begin = time.time()
        # gen accurate terms
        accurate_termids = self._get_accuate_termids(q)
        q_rep = self._tfidf.transform([q])
        blur_q = self._tt(q)
        # blur_q.sort(key=lambda x:-len(x))

        if len(accurate_termids) > 0:
            q_rep[:, accurate_termids] = q_rep[:, accurate_termids] * 2

        result = {}
        for docid in docids:
            result[docid] = self._gen_snippet(docid, q_rep, blur_q)

        # cost = time.time() - begin
        # print('[SNIPPET] cost ' + str(cost) + "s.")
        return result

    def snippet(self, docid, q):
        # begin = time.time()

        sents = self._docs[docid]["sents"]
        if len(sents) == 0:
            return ""
        accurate_termids = self._get_accuate_termids(q)
        q_rep = self._tfidf.transform([q])
        if len(accurate_termids) > 0:
            q_rep[:, accurate_termids] = q_rep[:, accurate_termids] * 2
        blur_q = set(self._tt(q))

        snpt = self._gen_snippet(docid, q_rep, blur_q)

        # cost = time.time() - begin
        # print('[SNIPPET] cost ' + str(cost) + "s.")

        return highlighted

'''
if __name__ == "__main__":
    import os
    import sys
    srvid = int(sys.argv[1])
    docs = pickle.load(open(os.path.join(manifest.DATA_DIR, "docs_%d.pkl" % srvid), 'rb'))
    tfidf = pickle.load(open(os.path.join(manifest.DATA_DIR, "tfidf.pkl"), 'rb'))

    snippeter = Snippeter(tfidf, docs, srvid)
    print(snippeter.snippet(int(sys.argv[2]), sys.argv[3]))
'''
