'''
index_broker.py

orchestrate index process in each node.
'''

from tornado.web import RequestHandler
from tornado.ioloop import IOLoop
from tornado.httpclient import AsyncHTTPClient
from tornado.gen import coroutine

from reformatter import Reformatter

'''
Tasks:
- copy all assigned raw file to local
- run inverted-index mapreduce job
'''

class ReformatBroker:
    def run(self, urls):
        yield 

class ReformatReqHandler(RequestHandler):
    def get(self):
        urls = self.get_argument("urls")



