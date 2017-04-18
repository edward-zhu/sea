#!/usr/bin/env python
# encoding: utf-8


import json
import time
from functools import reduce
from tornado.escape import url_escape
from tornado.web import RequestHandler, Application
from tornado.httpclient import AsyncHTTPClient
from tornado.gen import coroutine
from master import manifest

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
        #self._ioloop = IOLoop.current()

    def check_heartbeat(self):
        for host in self._hosts:
            if host.since_last_hbt() > self.HBT_DEADLINE:
                self._hosts.remove(host)

    def add_host(self, host):
        h = Host(host)
        self._hosts.append(h)
        print("Add new host %s" % host)

    def heartbeat_received(self, host):
        if host in self._hosts:
            self._hosts[host].update_hbt()
            return

        self.add_host(host)

    def return_hosts(self):
        hosts = [host for host in self._hosts]
        return hosts


#tracker = None
ht = HostTracker()

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
        if q == "":
            self.render("index.html")
        else:
            http_client = AsyncHTTPClient()
            begin = time.time()
            reps = yield http_client.fetch(manifest.MASTER + "/search?q=" + url_escape(q))
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
        q = self.get_argument("q")
        q = url_escape(q)
        begin = time.time()
        results = yield self.__fetch_results(q)
        results = results[:manifest.MAX_DOC_PER_QUERY]
        cost = time.time() - begin
        print("[MASTER] find results cost %.4fs." % cost)

        self.write({"results": results, "num_results": len(results)})

def make_master_app():
    return Application([
        (r"/", MainHandler),
        (r"/search", QueryHandler),
        (r'/heartbeat', HeartbeatReqHandler),
    ], template_path="static/")

if __name__ == '__main__':
    import tornado.ioloop
    app = make_master_app()
    app.listen(11111)
    tornado.ioloop.IOLoop.current().start()
