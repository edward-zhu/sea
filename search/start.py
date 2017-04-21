#!/usr/bin/env python
# encoding: utf-8


from tornado.web import RequestHandler, Application
from tornado.httpserver import HTTPServer
from tornado.httpclient import AsyncHTTPClient
from tornado.ioloop import IOLoop
from tornado.netutil import bind_sockets
from tornado.process import fork_processes

import socket
import re

import search.manifest as manifest
from search.frontend_app import make_frontend_app
from search.index_app import make_index_app
from search.doc_app import make_doc_app

index_apps = {}
doc_apps = {}
index_srvs = {}
doc_srvs = {}
frontend_app = None

def get_port(url):
    return int(re.findall(r':([0-9]+)', url)[0])

def start_indexer(i):
    port = get_port(manifest.INDEX_SRV[i])
    index_apps[i] = make_index_app(i)
    index_apps[i].listen(port)

if __name__ == "__main__":
    host = socket.gethostname()

    index_sockets = [bind_sockets(get_port(x)) for x in manifest.INDEX_SRV]
    doc_sockets = [bind_sockets(get_port(x)) for x in manifest.DOC_SRV]
    frontend_socket = bind_sockets(get_port(manifest.FRONTEND))

    for i in range(0, manifest.N_INDEX_SRV):
        port = get_port(manifest.INDEX_SRV[i])
        index_apps[i] = make_index_app(i)
        index_srvs[i] = HTTPServer(index_apps[i])
        index_srvs[i].add_sockets(index_sockets[i])
        print("[INDEX SRV] #%d listening on %s:%d." % (i, host, port))

    for i in range(0, manifest.N_DOC_SRV):
        port = get_port(manifest.DOC_SRV[i])
        doc_apps[i] = make_doc_app(i)
        doc_srvs[i] = HTTPServer(doc_apps[i])
        doc_srvs[i].add_sockets(doc_sockets[i])
        print("[DOC SRV] #%d listening on %s:%d." % (i, host, port))

    frontend_app = make_frontend_app()
    port = get_port(manifest.FRONTEND)
    frontend_srv = HTTPServer(frontend_app)
    frontend_srv.add_sockets(frontend_socket)

    print("[FRONT SRV] listening on port %s:%d." % (host, port))

    IOLoop.current().start()
