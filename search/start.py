#!/usr/bin/env python
# encoding: utf-8

from tornado.httpclient import AsyncHTTPClient
from tornado.ioloop import IOLoop,PeriodicCallback
from multiprocessing import Process

import re
import socket
import sys

import search.manifest as manifest
from search.frontend_app import make_frontend_app
from search.index_app import make_index_app
from search.doc_app import make_doc_app
from tornado.gen import coroutine, sleep

HEARTBEAT_INT = 2000 # heartbeat timeout in msec.
TRUNCATED_SEC = 30 # truncated to seconds

#MASTER_TRACKER = "http://localhost:%d" % (manifest.MASTER_PORT, )
MASTER_TRACKER = manifest.MASTER_TRACKER
PeCALL = None

def get_port(url):
    return int(re.findall(r':([0-9]+)', url)[0])

def get_internal_ip():
    return socket.gethostbyname(socket.gethostname())
    
HOST = "http://%s:%d" % (get_internal_ip(), get_port(manifest.FRONTEND), )

@coroutine    
def _send_req(req, retry=False):
    http_cli = AsyncHTTPClient()
    # truncated exponential backoff
    delay = 1
    while True:
        try:
            print("req: ", MASTER_TRACKER + req)
            ret = yield http_cli.fetch(MASTER_TRACKER + req)
        except Exception as e:
            if retry and delay < TRUNCATED_SEC:
                print("send req failed: %s, retry after %d sec." % (str(e), delay,))
                yield sleep(delay)
                delay *= 2
                continue
            else:
                print("send req failed.")
                return False, ""
        return True, ret

@coroutine 
def heartbeat():
    ok, res = yield _send_req("/heartbeat?host=" + HOST)
    if not ok:
        print('Warning: connect to master failed.')


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
    PeriodicCallback(heartbeat, HEARTBEAT_INT).start()
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
   
    
    
