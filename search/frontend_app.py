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


class MainHandler(RequestHandler):
    @coroutine
    def get(self):
        q = self.get_argument("q", "")
        if q == "":
            self.render("index.html")
        else:
            http_cli = AsyncHTTPClient()
            begin = time.time()
            reps = yield http_cli.fetch(manifest.FRONTEND + "search?q=" + url_escape(q))
            cost = time.time() - begin
            results = json.loads(reps.body)
            count = results["num_results"]

            self.render("result.html", cost=cost, count=count, results=results["results"])

class QueryHandler(RequestHandler):
    @coroutine
    def __fetch_indexes(self, q):
        http_cli = AsyncHTTPClient()
        reqs = [isrv + "index?q=" + q for isrv in manifest.INDEX_SRV]
        reps = yield [http_cli.fetch(req) for req in reqs]
        indexes = reduce(lambda x, y: x + json.loads(y.body)["postings"], reps, []);
        indexes.sort(key=lambda x: x[1], reverse=True);

        return indexes

    def __get_doc_url(self, srvid, docids, q):
        return manifest.DOC_SRV[srvid] + "doc?id=%s&q=%s" % (",".join(docids), q);

    @coroutine
    def __fetch_docs(self, indexes, q):
        http_cli = AsyncHTTPClient()
        nsrv = manifest.N_DOC_SRV
        docids = {}
        for i in range(0, nsrv):
            docids[i] = []
        for ind in indexes:
            docids[hashf(str(ind[0])) % nsrv].append(str(ind[0]))
        reqs = [self.__get_doc_url(i, docids[i], q) for i in docids if len(docids[i]) > 0]

        reps = yield [http_cli.fetch(req) for req in reqs]

        results = reduce(lambda x, y: x + json.loads(y.body)["results"], reps, [])

        docs = {}
        for r in results:
            docs[r["doc_id"]] = r

        return docs

    @coroutine
    def get(self):
        q = self.get_argument("q")
        q = url_escape(q)
        begin = time.time()
        indexes = yield self.__fetch_indexes(q)
        indexes = indexes[:manifest.MAX_DOC_PER_QUERY]
        cost = time.time() - begin
        print("[FRONT END] indexer cost %.4fs." % cost)

        docs = yield self.__fetch_docs(indexes, q)

        docs = [dict(docs[x[0]], score=x[1]) for x in indexes]
    
        cost = time.time() - begin
        print("[FRONT END] doc cost %.4fs." % cost)
        self.write({"results":docs, "num_results": len(docs)})

def make_frontend_app():
    return Application([
        (r"/", MainHandler),
        (r"/search", QueryHandler),
    ], template_path="static/")

if __name__ == '__main__':
    import tornado.ioloop
    app = make_frontend_app()
    app.listen(22333)
    tornado.ioloop.IOLoop.current().start()
