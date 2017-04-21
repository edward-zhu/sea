#!/usr/bin/env python
# encoding: utf-8


import json
import time
import math
from functools import reduce
from tornado.escape import url_escape
from tornado.web import RequestHandler, Application
from tornado.httpclient import AsyncHTTPClient
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.gen import coroutine
from master import manifest

ht = None
index_cache = {}
#doc_cache = {}

class Host:
    def __init__(self, host, data_id):
        self._host = host
        self._dataid = data_id
        self._lasthbt = time.time()

    def update_hbt(self):
        self._lasthbt = time.time()

    def since_last_hbt(self):
        return time.time() - self._lasthbt

    @property
    def host(self):
        return self._host

    @property
    def dataid(self):
        return self._dataid

class HostTracker:
    HBT_DEADLINE = 30  # heartbeat timeout in second

    def __init__(self):
        self._hosts = manifest.FRONT_ENDS  # initially no host is visible or available
        self._ioloop = None

    def check_heartbeat(self):
        for host in self._hosts:
            if host.since_last_hbt() > self.HBT_DEADLINE:
                self._hosts.remove(host)
                print("%s lost connections" % host.host)
        self.show_hosts()

    def find_host_with_dataid(self, dataid):
        for host in self._hosts:
            if host.dataid == dataid:
                return host.host
        return -1

    def add_host(self, host, dataid):
        h = Host(host, dataid)
        self._hosts.append(h)
        print("Add new host %s" % host)

    def find_index(self, host):
        for i in range(len(self._hosts)):
            if self._hosts[i].host == host:
                return i
        return -1

    def heartbeat_received(self, host, data_id):
        if host in self.return_hosts():
            i = self.find_index(host)
            self._hosts[i].update_hbt()
            return

        self.add_host(host, data_id)

    def return_hosts(self):
        hosts = [host.host for host in self._hosts]
        return hosts

    def show_hosts(self):
        hosts = [host.host for host in self._hosts]
        print(hosts)

    def setup(self):
        self._ioloop = IOLoop.current()
        self.hbt_timer = PeriodicCallback(self.check_heartbeat, HostTracker.HBT_DEADLINE * 1000)
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
            reps = yield http_client.fetch(manifest.MASTER + "/search?q=" + url_escape(q) + "&page=" + str(page))
            cost = time.time() - begin
            results = json.loads(str(reps.body, encoding="utf-8"))
            count = results["num_results"]

            self.render("result.html", cost=cost, count=count, results=results["results"])

class QueryHandler(RequestHandler):
    @coroutine
    def __fetch_indexes(self, q):
        http_client = AsyncHTTPClient()
        reqs = [host + "/search?q=" + q for host in ht.return_hosts()]
        print(reqs)
        reps = yield [http_client.fetch(req) for req in reqs]
        indexes = reduce(lambda x, y: x + json.loads(y.body)["results"], reps, []);
        indexes.sort(key=lambda x: x[1], reverse=True);

        return indexes

    def __get_doc_url(self, srvh, docids, q):
        return srvh + "/doc?id=%s&q=%s" % (",".join(docids), q);

    @coroutine
    def __fetch_docs(self, indexes, q):
        http_client = AsyncHTTPClient()
        docids = {}
        for host in ht.return_hosts():
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
        reqs = [self.__get_doc_url(host, docids[host], q) for host in ht.return_hosts() if len(docids[host]) > 0]

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
    ], template_path="static/")
    ht.setup()
    return app

if __name__ == '__main__':
    import tornado.ioloop
    app = make_master_app()
    app.listen(11111)
    tornado.ioloop.IOLoop.current().start()
