#!/usr/bin/env python
# encoding: utf-8

from tornado.web import RequestHandler, Application, MissingArgumentError
from tornado.httpclient import AsyncHTTPClient
from tornado.process import Subprocess
from tornado.ioloop import IOLoop
from tornado.concurrent import run_on_executor
from tornado.gen import coroutine, Task

import json
import hashlib
import time
import subprocess
import os
import bz2
import io
import signal
import threading
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from operator import itemgetter

from mapreduce.utils import hashf
from mapreduce import utils
from mapreduce import manifest

SALT = "sfkslfdsf"
results = {}

def gen_taskid(input_file):
    return hashlib.md5((input_file + str(time.time()) + SALT).encode("utf-8")).hexdigest()

env = os.environ.copy()
executor = ThreadPoolExecutor()

def decompress(input_file, pipe):
    with bz2.open(input_file, "r") as f:
        for line in f:
            pipe.write(line)
    pipe.close()

def compress(output_file, pipe):
    with bz2.open(output_file, "w") as f:
        for line in pipe:
            f.write(line)
    pipe.close()

def runMapper(exec_file, input_file, num_reducers):
    result = [[] for i in range(0, num_reducers)]

    f = None
    if input_file[-4:] == ".bz2":
        proc = subprocess.Popen([exec_file], stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                env=env)
        threading.Thread(target=decompress, args=(input_file, proc.stdin)).start()
    else:
        f = open(input_file, "r", encoding="utf-8")
        proc = subprocess.Popen([exec_file], stdin=f,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                env=env)

    out_by_part = defaultdict(list)

    for line in proc.stdout:
        kv = line.decode("utf-8").split("\t")
        out_by_part[kv[0]].append([kv[0], kv[1]])

    for line in proc.stderr:
        print("map error:\t", line)

    # close file handlers
    if f is not None:
        f.close()

    proc.stdout.close()
    proc.stderr.close()

    ret = proc.wait()

    if ret != 0:
        return ret, {}

    # rid = hashf(kv[0]) % num_reducers
    for k in sorted(out_by_part.keys()):
        rid = hashf(k) % num_reducers
        result[rid].extend(out_by_part[k])

    return ret, result

class MapHandler(RequestHandler):
    @coroutine
    def get(self):
        try:
            mapper_path = self.get_argument("mapper_path")
            input_file = self.get_argument("input_file")
            num_reducers = int(self.get_argument("num_reducers"))
        except MissingArgumentError as err:
            self.write({"status": "failed", "error": str(err)})
            return

        task_id = gen_taskid(input_file)

        try:
            ret, result = yield executor.submit(runMapper, mapper_path, input_file, num_reducers)
        except Exception as e:
            self.write({"status": "failed", "error": "call mapper failed: %s" % str(e)})
            return

        print("%s map task:%s done." % (self.request.host, input_file))
        if ret != 0:
            self.write({
                "status": "failed",
                "error" : "call mapper failed, returns: %d" % ret,
                "map_task_id": task_id})
            return

        results[task_id] = result
        self.write({"status": "success", "map_task_id": task_id})

# invoke by reducer return output key-value pairs
class RetriveMapOutputHandler(RequestHandler):
    def get(self):
        try:
            reducer_ix = int(self.get_argument("reducer_ix"))
            task_id = self.get_argument("map_task_id")
        except MissingArgumentError as err:
            self.write({"status": "failed", "error": str(err)})

        if task_id not in results:
            self.set_status(400, "invalid task id %d" % task_id)
            self.write("[]")
            return

        if reducer_ix < 0 or reducer_ix >= len(results[task_id]):
            self.set_status(400, "invalid reducer id %d" % reducer_ix)
            self.write("[]")
            return
        self.write(json.dumps(results[task_id][reducer_ix]))
        results[task_id][reducer_ix] = None

def gen_map_requests(rix, cli, ids):
    worker_urls = utils.worker_urls()
    n_workers = manifest.WORKER_NUM
    urls = ["%s/retrive_map_output?reducer_ix=%d&map_task_id=%s" %
            (worker_urls[i % n_workers], rix, _id) for i, _id in enumerate(ids)]

    return [cli.fetch(url, connect_timeout=6000, request_timeout=6000) for url in urls]

from functools import reduce

def runReducer(exec_file, input_data, output_path, reducer_ix):
    err_file = os.path.join(output_path, "%d.err" % reducer_ix)
    ef = open(err_file, "wb")

    f = None

    if manifest.COMPRESS_OUTPUT:
        output_file = os.path.join(output_path, "%d.out.bz2" % reducer_ix)
        proc = subprocess.Popen([exec_file],
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=ef, env=env)
        threading.Thread(target=compress, args=(output_file, proc.stdout)).start()
    else:
        output_file = os.path.join(output_path, "%d.out" % reducer_ix)
        f = open(output_file, "wb")
        proc = subprocess.Popen([exec_file], stdin=subprocess.PIPE, stdout=f, stderr=ef, env=env)

    proc.stdin.write(input_data)
    proc.stdin.close()

    ret = proc.wait()

    if f is not None:
        f.close()

    ef.close()

    return ret

class ReduceHandler(RequestHandler):
    @coroutine
    def get(self):
        try:
            reducer_ix = int(self.get_argument("reducer_ix"))
            reducer_path = self.get_argument("reducer_path")
            ids = self.get_argument("map_task_ids").split(",")
            output_path = self.get_argument("output_path")
            input_path = self.get_argument("input_path")
        except MissingArgumentError as err:
            self.write({"status" : "failed", "error" : str(err)})
            return

        cli = AsyncHTTPClient()

        res = yield gen_map_requests(reducer_ix, cli, ids)

        map_res = []
        for r in res:
            if r.code != 200:
                self.write({"status" : "failed", "error" : "Fetch map result failed: %d" % r.code})
                return
            #map_res.append(json.loads(str(r.body, encoding="utf-8")))
            map_res.extend(json.loads(str(r.body, encoding="utf-8")))

        map_res.sort(key=itemgetter(0))
        #merged_res = heapq.merge(*map_res, key=itemgetter(0))
        input_data = "\n".join(map(lambda x: "\t".join(x).strip(), map_res)).encode("utf-8")

        ret = 0
        try:
            ret = yield executor.submit(runReducer,
                                        reducer_path, input_data, output_path, reducer_ix)
        except Exception as e:
            self.write({"status": "failed", "error" : "Reducer run failed: %s" % str(e)})
            return

        print("%s reduce done." % self.request.host)

        if ret != 0:
            self.write({"status" : "failed", "error" : "Reducer run failed returns: %d" % ret})
            return
        self.write({"status": "success"})

def make_worker_app():
    return Application([
        (r"/map", MapHandler),
        (r"/retrive_map_output", RetriveMapOutputHandler),
        (r"/reduce", ReduceHandler),
    ])

@coroutine
def main():
    ret, res = yield runMapper("wordcount/mapper.py", "fish_jobs/0.in", 4)
    print(res)
    IOLoop.instance().stop()

def shutdown():
    serv.stop()
    print("server stopped.")
    ioloop = IOLoop.instance()
    ioloop.stop()
    print("ioloop stopped")

def sigterm_hdl(signo, stack):
    print("got stop signal %d" % (signo, ))
    ioloop = IOLoop.instance()
    ioloop.add_callback(shutdown)

if __name__ == "__main__":
    import sys
    signal.signal(signal.SIGTERM, sigterm_hdl)
    signal.signal(signal.SIGTSTP, sigterm_hdl)
    signal.signal(signal.SIGINT, sigterm_hdl)

    AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")

    app = make_worker_app()
    port = 8080 if len(sys.argv) == 1 else int(sys.argv[1])
    global serv
    serv = app.listen(port)
    print("worker %d listen on %s" % (port, utils.worker_url(port - manifest.BASE_PORT)))
    IOLoop.current().start()

    print("exited")
    sys.exit(0)
