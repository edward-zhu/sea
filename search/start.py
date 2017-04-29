#!/usr/bin/env python
# encoding: utf-8

import re
import socket
import sys
import time
import logging
import signal
import etcd

from search.common import get_etcd_cli

from multiprocessing import Process, SimpleQueue
from threading import Thread

from tornado.ioloop import IOLoop, PeriodicCallback


from search.config import EtcdConfigFactory
import search.manifest as manifest
from search.frontend_app import make_frontend_app
from search.index_app import make_index_app
from search.doc_app import make_doc_app
from tornado.gen import coroutine, sleep

HEARTBEAT_INT = 2000 # heartbeat timeout in msec.
TRUNCATED_SEC = 30 # truncated to seconds

HOST = socket.gethostname()
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("etcd").setLevel(logging.WARNING)


class SearchEngine:
    '''search engine class'''
    def __init__(self, cfg):
        self.cfg = cfg
        self.etcd_cli = get_etcd_cli()
        self.srvs = []
        self.ready = False
        self.timer = None

    def _srv_desc(self):
        return self.cfg.srv_desc

    def shutdown(self, signo, frame):
        '''shutdown search engine'''
        print("shutting down..")
        self.etcd_cli.delete("/misaki/srvs/%d" % (self.cfg.srvid, ))
        self.etcd_cli.delete("/misaki/avail/%s" % self._srv_desc())
        for srv in self.srvs:
            srv.terminate()

    def _heartbeat_impl(self):
        try:
            self.etcd_cli.write(
                "/misaki/srvs/%d" % (self.cfg.srvid, ),
                self._srv_desc(),
                ttl=10,
                prevValue=self._srv_desc())
        except etcd.EtcdCompareFailed:
            logger.warning("extend lease failed, shutdown...")

        if not self.ready:
            return

        self.etcd_cli.write(
            "/misaki/avail/%s" % self._srv_desc(),
            value=1,
            ttl=10
        )

    def _heartbeat(self):
        while True:
            self._heartbeat_impl()
            time.sleep(0.5)

    def _start_index_app(self, i, queue):
        port = self.cfg.index_srv_port(i)
        app = make_index_app(
            self.cfg.get_tfidf(),
            self.cfg.get_index_data(i),
            self.cfg.max_q_doc
        )
        queue.put(i)
        print("[INDEX SRV] #%d listening on %s:%d." % (i, HOST, port))
        app.listen(port)
        IOLoop.current().start()

    def _start_doc_app(self, i, queue):
        port = self.cfg.doc_srv_port(i)
        app = make_doc_app(
            self.cfg.get_doc_data(i),
            self.cfg.get_tfidf(),
            self.cfg.snippet_len
        )
        print("[DOC SRV] #%d listening on %s:%d." % (i, HOST, port))
        queue.put(i + 10)
        app.listen(port)
        IOLoop.current().start()

    def _start_frontend_app(self):
        port = self.cfg.front_port
        app = make_frontend_app(self.cfg)
        print("[FRONT SRV] listening on port %s:%d." % (HOST, port))
        app.listen(port)
        IOLoop.current().start()

    def start(self):
        queue = SimpleQueue()

        for i in range(0, self.cfg.n_index):
            srv = Process(target=self._start_index_app, args=(i, queue,))
            self.srvs.append(srv)

        for i in range(0, self.cfg.n_doc):
            srv = Process(target=self._start_doc_app, args=(i, queue,))
            self.srvs.append(srv)

        for srv in self.srvs:
            srv.start()

        self.timer = Thread(target=self._heartbeat)
        self.timer.daemon = True
        self.timer.start()

        count = 0
        while count < self.cfg.n_index + self.cfg.n_doc:
            queue.get()
            count += 1

        self.ready = True

        frontend_srv = Process(target=self._start_frontend_app)
        frontend_srv.start()

        self.srvs.append(frontend_srv)

        signal.signal(signal.SIGQUIT, self.shutdown)
        signal.signal(signal.SIGTSTP, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

        try:
            for srv in self.srvs:
                srv.join()
        finally:
            print("program exit!")
            return

if __name__ == "__main__":
    fac = EtcdConfigFactory()
    cfg = fac.get_cfg()
    se = SearchEngine(cfg)
    se.start()

