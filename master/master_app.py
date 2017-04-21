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
#from cachetools import LRUCache

ht = None
result_cache = {}

class Host:
    def __init__(self, host):
        self._host = host
        self._lasthbt = time.time()

    def update_hbt(self):
        self._lasthbt = time.time()

    def since_last_hbt(self):
        return time.time() - self._lasthbt

    @property
    def host(self):
        return self._host

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

    def add_host(self, host):
        h = Host(host)
        self._hosts.append(h)
        print("Add new host %s" % host)

    def find_index(self, host):
        for i in range(len(self._hosts)):
            if self._hosts[i].host == host:
                return i
        return -1

    def heartbeat_received(self, host):
        if host in self.return_hosts():
            i = self.find_index(host)
            self._hosts[i].update_hbt()
            return

        self.add_host(host)

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
        ht.heartbeat_received(host)
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
    def __fetch_results(self, q):
        http_client = AsyncHTTPClient()
        reqs = [host + "/search?q=" + q for host in ht.return_hosts()]
        reps = yield [http_client.fetch(req) for req in reqs]
        #for rep in reps:
        #     print(str(rep.body))
        results = reduce(lambda x, y: x + json.loads(str(y.body, encoding="utf-8"))["results"], reps, [])
        results.sort(key=lambda x: x["score"], reverse=True)
        return results


    @coroutine
    def get(self):
        global result_cache
        q = self.get_argument("q")
        q = url_escape(q)
        page = self.get_argument("page")
        page = int(url_escape(page))
        begin = time.time()
        if q in result_cache.keys():
            result_list = result_cache[q]["results"]
            length = result_cache[q]["num_results"]
        else:
            results = yield self.__fetch_results(q)
            #results = results[:manifest.MAX_DOC_PER_QUERY]
            length = len(results)
            result_list = []
            end = int(math.ceil(float(len(results)/10)))
            for i in range(0, end):
                result_list.append(results[i*10 : (i+1)*10])
            result_cache[q] = {"results": result_list, "num_results": length}
        cost = time.time() - begin
        print("[MASTER] find results cost %.4fs." % cost)

        if page >= len(result_list):
            self.write({"results": [], "num_results": length, "num_pages": len(result_list)})
        else:
            self.write({"results": result_list[page], "num_results": length, "num_pages": len(result_list)})

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
