#!/usr/bin/env python
# encoding: utf-8


import json
import time
from functools import reduce
from tornado.escape import url_escape
from tornado.web import RequestHandler, Application
from tornado.httpclient import AsyncHTTPClient
from tornado.gen import coroutine

from search import manifest
from mapreduce.utils import hashf

class DocsHandler(RequestHandler):
    def __get_doc_url(self, srvid, docids, q):
        return manifest.DOC_SRV[srvid] + "doc?id=%s&q=%s" % (",".join(docids), q)

    @coroutine
    def __fetch_docs(self, indexes, q):
        http_cli = AsyncHTTPClient()
        nsrv = manifest.N_DOC_SRV
        docids = {}
        for i in range(0, nsrv):
            docids[i] = []
        for ind in indexes:
            docids[hashf(str(ind)) % nsrv].append(str(ind))
        reqs = [self.__get_doc_url(i, docids[i], q) for i in docids if len(docids[i]) > 0]

        reps = yield [http_cli.fetch(req) for req in reqs]

        results = reduce(lambda x, y: x + json.loads(str(y.body, encoding="utf-8"))["results"], reps, [])

        docs = {}
        for r in results:
            docs[r["doc_id"]] = r

        return docs

    @coroutine
    def get(self):
        docids = self.get_argument("id")
        q = self.get_argument("q")
        q = url_escape(q)
        docids = [int(x) for x in docids.split(",")]

        docs = yield self.__fetch_docs(docids, q)

        self.write({"results":docs})

class QueryHandler(RequestHandler):
    @coroutine
    def __fetch_indexes(self, q):
        http_cli = AsyncHTTPClient()
        reqs = [isrv + "index?q=" + q for isrv in manifest.INDEX_SRV]
        reps = yield [http_cli.fetch(req) for req in reqs]
        indexes = reduce(lambda x, y: x + json.loads(str(y.body, encoding="utf-8"))["postings"], reps, [])
        indexes.sort(key=lambda x: x[1], reverse=True)
        indexes = [["%s_%d" % (manifest.DATA_ID, x[0],), x[1]] for x in indexes]

        return indexes

    @coroutine
    def get(self):
        q = self.get_argument("q")
        q = url_escape(q)
        begin = time.time()
        indexes = yield self.__fetch_indexes(q)
        indexes = indexes[:manifest.MAX_DOC_PER_QUERY]

        self.write({"results":indexes, "num_results": len(indexes)})

def make_frontend_app():
    return Application([
        (r"/search", QueryHandler),
        (r"/doc", DocsHandler),
    ], template_path="static/")

if __name__ == '__main__':
    import tornado.ioloop
    app = make_frontend_app()
    app.listen(22333)
    tornado.ioloop.IOLoop.current().start()
