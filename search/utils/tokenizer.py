#!/usr/bin/env python
# encoding: utf-8

import re
from nltk.stem import SnowballStemmer, WordNetLemmatizer
from nltk.tokenize import RegexpTokenizer

class SimpleTokenizer:
    def __init__(self):
        self.rt = RegexpTokenizer(r'(?u)\b\w\w+\b')

    def __call__(self, doc):
        return [x for x in self.rt.tokenize(doc)]

class WordNetTokenizer:
    def __init__(self):
        self.wnl = WordNetLemmatizer()
        self.rt = RegexpTokenizer(r'(?u)\b\w\w+\b')

    def __call__(self, doc):
        return [x for t in self.rt.tokenize(doc) for x in [t, self.wnl.lemmatize(t)]]


class SideEffectTokenizer:
    def __init__(self, stem=False):
        self.stemmer = None
        if stem == True:
            self.stemmer = SnowballStemmer('english')
        self.pattern = r'(?u)\b\w\w+\b'
        self.rt = RegexpTokenizer(self.pattern)
        self.RE = re.compile(self.pattern)

    def _stem(self, x):
        if self.stemmer is None:
            return x
        else:
            return self.stemmer.stem(x)

    def __call__(self, doc, output_pos=False):
        tokens = []
        if output_pos:
            poses = {}
            for m in self.RE.finditer(doc):
                tok = self._stem(m.group(0))
                tokens.append(tok)
                if tok not in poses:
                    poses[tok] = []
                poses[tok].append([m.start(), m.end()])
            return tokens, poses

        for t in self.rt.tokenize(doc):
            tokens.append(self._stem(t))
            if (self.stem(t) != t):
                tokens.append(t)

class StemTokenizer:
    def __init__(self):
        self.stemmer = SnowballStemmer('english')
        self.rt = RegexpTokenizer(r'(?u)\b\w\w+\b')

    def __call__(self, doc, keep=True):
        tokens = []
        for t in self.rt.tokenize(doc):
            tokens.append(t)
            stemmed = self.stemmer.stem(t)
            if t != stemmed and keep:
                tokens.append(stemmed)
        return tokens

Tokenizer = StemTokenizer
