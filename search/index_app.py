#!/usr/bin/env python
# encoding: utf-8

from tornado.web import RequestHandler, Application
from tornado.ioloop import IOLoop

from search.scorer import make_scorer

class QueryHandler(RequestHandler):
    def initialize(self, scorer, max_q_doc):
        self.scorer = scorer
        self.max_q_doc = max_q_doc

    def get(self):
        q = self.get_argument("q")
        scores = self.scorer.scores(q)[:self.max_q_doc]
        self.write({"postings": scores})

def make_index_app(tfidf_f, data_f, max_q_doc):
    scorer = make_scorer(tfidf_f, data_f)
    app = Application([
        (r'/index', QueryHandler, dict(scorer=scorer, max_q_doc=max_q_doc)),
    ])

    return app

'''
import re

def get_port(url):
    return int(re.findall(r':([0-9]+)', url)[0])

if __name__ == '__main__':
    import sys
    srv_id = int(sys.argv[1])
    app = make_index_app(srv_id)
    app.listen(get_port(manifest.INDEX_SRV[srv_id]))
    IOLoop.current().start()
'''
