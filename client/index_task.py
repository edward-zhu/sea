'''
index_broker.py

orchestrate index process in each node.
'''

from tornado.ioloop import IOLoop
from tornado.httpclient import AsyncHTTPClient
from tornado.gen import coroutine

'''
Responsibility:
- send heartbeat to master
- receive job spec.
- report job state

Tasks:
- copy all reformatted file to local
- run inverted-index mapreduce job
- run partial idf mapreduce job
- fetch global idf from master
- run doc mapreduce job
- run integerate.py

* use GCS FUSE can avoid many upload/download code
'''

class IndexBroker:
