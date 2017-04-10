#!/usr/bin/env python

import sys
import json
from nltk.corpus import stopwords

from search.utils.tokenizer import StemTokenizer

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