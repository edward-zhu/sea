#!/usr/bin/env python
# encoding: utf-8


import json
import time
import math

import etcd

from functools import reduce
from tornado.escape import url_escape
from tornado.web import RequestHandler, StaticFileHandler, Application
from tornado.httpclient import AsyncHTTPClient
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.gen import coroutine
from master import manifest

ht = None
index_cache = {}
#doc_cache = {}

class HostTracker:
    HBT_DEADLINE = 1

    def __init__(self):
        self._hosts = {}  # initially no host is visible or available
        self._ioloop = None
        self.etcd_cli = etcd.Client()

    def _update_hosts(self):
        host_candidates = self.etcd_cli.read('/misaki/srvs').children
        self._hosts = {}
        for host in host_candidates:
            dataid = host.key.split("/")[-1]
            h = host.value
            try:
                self.etcd_cli.read('/misaki/avail/%s' % (h,))
                self._hosts[dataid] = h
            except etcd.EtcdKeyNotFound:
                pass

        self.show_hosts()

    def find_host_with_dataid(self, dataid):
        return self._hosts.get(dataid, -1)

    def hosts(self):
        return self._hosts.values()

    def show_hosts(self):
        print(self._hosts)

    def setup(self):
        self._ioloop = IOLoop.current()
        self.hbt_timer = PeriodicCallback(self._update_hosts, HostTracker.HBT_DEADLINE * 1000)
        self.hbt_timer.start()

class HeartbeatReqHandler(RequestHandler):
    @coroutine
    def get(self):
        host = self.get_argument('host')
        data_id = self.get_argument('srvid')
        ht.heartbeat_received(host, data_id)
        self.finish({"status" : "ok"})

class MainHandler(RequestHandler):
    @coroutine
    def get(self):
        q = self.get_argument("q", "")
        page = int(self.get_argument("page", "0"))
        if q == "":
            self.render("index.html")
        else:
            http_client = AsyncHTTPClient()
            begin = time.time()
            reps = yield http_client.fetch(
                manifest.MASTER + "/search?q=" + url_escape(q) + "&page=" + str(page))
            cost = time.time() - begin
            results = json.loads(str(reps.body, encoding="utf-8"))
            count = results["num_results"]

            self.render("result.html", cost=cost,
                        count=count,
                        results=results["results"],
                        pages=range(0, results["num_pages"]), cur_page=page, q=q)

class QueryHandler(RequestHandler):
    @coroutine
    def __fetch_indexes(self, q):
        http_client = AsyncHTTPClient()
        reqs = [host + "/search?q=" + q for host in ht.hosts()]
        print(reqs)
        reps = yield [http_client.fetch(req) for req in reqs]
        indexes = reduce(lambda x, y: x + json.loads(y.body)["results"], reps, [])
        indexes.sort(key=lambda x: x[1], reverse=True)

        return indexes

    def __get_doc_url(self, srvh, docids, q):
        return srvh + "/doc?id=%s&q=%s" % (",".join(docids), q)

    @coroutine
    def __fetch_docs(self, indexes, q):
        http_client = AsyncHTTPClient()
        docids = {}
        for host in ht.hosts():
            docids[host] = []
        for ind in indexes:
            temp = ind[0].split('_')
            server_host = ht.find_host_with_dataid(temp[0])
            doc_id = temp[1]
            if server_host == -1:
                print("No such server whose data id is %d" % ind[2])
            else:
                docids[server_host].append(doc_id)
            #docids[hashf(str(ind[0])) % nsrv].append(str(ind[0]))
        reqs = [self.__get_doc_url(host, docids[host], q)
                for host in ht.hosts() if len(docids[host]) > 0]

        reps = yield [http_client.fetch(req) for req in reqs]

        results = reduce(lambda x, y: {**x, **json.loads(y.body)["results"]}, reps, {})

        #docs = {}
        #for r in results:
        #    docs[r["doc_id"]] = r

        return results

    '''
    @coroutine
    def __fetch_results(self, q):
        http_client = AsyncHTTPClient()
        reqs = [host + "/search?q=" + q for host in ht.return_hosts()]
        reps = yield [http_client.fetch(req) for req in reqs]
        #for rep in reps:
        #     print(str(rep.body))
        results = reduce(lambda x, y: x + json.loads(str(y.body, encoding="utf-8"))["results"], reps, [])
        results.sort(key=lambda x: x["score"], reverse=True)
        return results
    '''


    @coroutine
    def get(self):
        global index_cache
        q = self.get_argument("q")
        q = url_escape(q)
        page = self.get_argument("page")
        page = int(url_escape(page))
        begin = time.time()
        if q in index_cache.keys():
            index_list = index_cache[q]["results"]
            length = index_cache[q]["num_results"]
        else:
            indexes = yield self.__fetch_indexes(q)
            indexes = indexes[:manifest.MAX_DOC_PER_QUERY]
            length = len(indexes)
            index_list = []
            end = int(math.ceil(float(length/10)))
            for i in range(0, end):
                index_list.append(indexes[i*10:(i+1)*10])
            index_cache[q] = {"results": index_list, "num_results": length}
        cost = time.time() - begin
        print("[MASTER] find results cost %.4fs." % cost)

        if page >= len(index_list):
            self.write({"results": [], "num_results": length, "num_pages": len(index_list)})
        else:
            docs = yield self.__fetch_docs(index_list[page], q)
            docs = [dict(docs[x[0].split('_')[1]], score=x[1]) for x in index_list[page]]
            self.write({"results": docs, "num_results": length, "num_pages": len(index_list)})

def make_master_app():
    global ht
    ht = HostTracker()
    app = Application([
        (r"/", MainHandler),
        (r"/search", QueryHandler),
        (r'/heartbeat', HeartbeatReqHandler),
        (r"/static/(.*)", StaticFileHandler, {"path": "static/"}),
    ], template_path="static/")
    ht.setup()
    return app

if __name__ == '__main__':
    import tornado.ioloop
    app = make_master_app()
    app.listen(11111)
    tornado.ioloop.IOLoop.current().start()
