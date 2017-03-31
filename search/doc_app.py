#!/usr/bin/env python
# encoding: utf-8

from tornado.web import RequestHandler, Application
from tornado.ioloop import IOLoop
import pickle
import os

from snippeter import Snippeter
import manifest

class QueryHandler(RequestHandler):
    def initialize(self, docs, snippeter):
        self.docs = docs
        self.snippeter = snippeter

    def _gen_results(self, docids, q):
        docids = docids.split(",")
        docids = [int(x) for x in docids]
        results = []

        snippets = self.snippeter.snippet_batch(docids, q)

        for docid in docids:
            results.append({
                "doc_id" : docid,
                "title" : self.docs[docid]["metadata"]["title"],
                "url" : self.docs[docid]["metadata"]["url"],
                "snippet" : snippets[docid],
            })

        return results

    def get(self):
        docids = self.get_argument("id")
        q = self.get_argument("q")
        self.write({
            "results" : self._gen_results(docids, q)
        })

def make_doc_app(srvid):
    docs = pickle.load(open(os.path.join(manifest.DATA_DIR, "docs_%d.pkl" % srvid), "rb"))
    tfidf = pickle.load(open(os.path.join(manifest.DATA_DIR, "tfidf.pkl"), "rb"))
    snippeter = Snippeter(tfidf, docs, srvid)
    app = Application([
        (r'/doc', QueryHandler, dict(docs=docs, snippeter=snippeter))
    ])

    return app

import re

def get_port(url):
    return int(re.findall(r':([0-9]+)', url)[0])

if __name__ == "__main__":
    import sys
    srv_id = int(sys.argv[1])
    app = make_doc_app(srv_id)
    app.listen(get_port(manifest.DOC_SRV[srv_id]))
    IOLoop.current().start()
