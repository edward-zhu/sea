#!/usr/bin/env python
# encoding: utf-8

from tornado.web import RequestHandler, Application
from tornado.ioloop import IOLoop

import manifest
from scorer import make_scorer

class QueryHandler(RequestHandler):
    def initialize(self, scorer):
        self.scorer = scorer

    def get(self):
        q = self.get_argument("q")
        scores = self.scorer.scores(q)[:manifest.MAX_DOC_PER_QUERY]
        self.write({"postings": scores})

def make_index_app(srvid):
    print("[INDEX SRV] generating scorer for index srv #%d..." % srvid)
    scorer = make_scorer(srvid)
    print("[INDEX SRV] scorer generated for index srv #%d." % srvid)
    app = Application([
        (r'/index', QueryHandler, dict(scorer=scorer)),
    ])

    return app

import re

def get_port(url):
    return int(re.findall(r':([0-9]+)', url)[0])

if __name__ == '__main__':
    import sys
    srv_id = int(sys.argv[1])
    app = make_index_app(srv_id)
    app.listen(get_port(manifest.INDEX_SRV[srv_id]))
    IOLoop.current().start()
