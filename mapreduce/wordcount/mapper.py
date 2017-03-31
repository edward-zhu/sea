#!/usr/bin/env python3

import sys
import nltk

for line in sys.stdin:
    for word in nltk.word_tokenize(line.strip()):
        print('%s\t%s' % (word, 1))
