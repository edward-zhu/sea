#!/usr/bin/env python

import sys
import json
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer
from nltk.tokenize import RegexpTokenizer

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

tokenize = StemTokenizer()

count = 0

stop = set(stopwords.words('english'))

for line in sys.stdin:
    data = json.loads(line)
    for token in filter(lambda x: x not in stop, tokenize(data["doc"])):
    # for token in tokenize(data["doc"]):
        print('%s\t%s' % (data["metadata"]["doc_id"], token))
    count += 1

print('#D\t%d' % count)