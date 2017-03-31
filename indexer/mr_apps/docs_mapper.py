#!/usr/bin/env python

import sys
import json

for line in sys.stdin:
    data = json.loads(line)
    print("%d\t%s" % (data["metadata"]["doc_id"], json.dumps(data)))
