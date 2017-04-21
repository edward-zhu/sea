#!/usr/bin/env python
# encoding: utf-8

from tornado.httpclient import AsyncHTTPClient
from tornado.ioloop import IOLoop
from multiprocessing import Process

import re
import socket
import sys

import search.manifest as manifest
from search.frontend_app import make_frontend_app
from search.index_app import make_index_app
from search.doc_app import make_doc_app


srvs = []

host = socket.gethostname()

def get_port(url):
    return int(re.findall(r':([0-9]+)', url)[0])

def start_index_app(i):
    port = get_port(manifest.INDEX_SRV[i])
    app = make_index_app(i)
    print("[INDEX SRV] #%d listening on %s:%d." % (i, host, port))
    app.listen(port)
    IOLoop.current().start()

def start_doc_app(i):
    port = get_port(manifest.DOC_SRV[i])
    app = make_doc_app(i)
    print("[DOC SRV] #%d listening on %s:%d." % (i, host, port))
    app.listen(port)
    IOLoop.current().start()

def start_frontend_app():
    port = get_port(manifest.FRONTEND)
    app = make_frontend_app()
    print("[FRONT SRV] listening on port %s:%d." % (host, port))
    app.listen(port)
    IOLoop.current().start()


if __name__ == "__main__":
    for i in range(0, manifest.N_INDEX_SRV):
        srv = Process(target=start_index_app, args=(i,))
        srvs.append(srv)
        

    for i in range(0, manifest.N_DOC_SRV):
        srv = Process(target=start_doc_app, args=(i,))
        srvs.append(srv)
        
    frontend_srv = Process(target=start_frontend_app)
    srvs.append(frontend_srv)
    
    for srv in srvs:
        srv.start()
    
    try:
        for srv in srvs:
            srv.join()
    finally:
        print("program exit!")
        sys.exit(0)
   
    
    
