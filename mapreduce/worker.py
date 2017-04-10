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
from concurrent.futures import ThreadPoolExecutor

from mapreduce.utils import hashf
from mapreduce import utils
from mapreduce import manifest

SALT = "sfkslfdsf"
results = {}

def gen_taskid(input_file):
    return hashlib.md5((input_file + str(time.time()) + SALT).encode("utf-8")).hexdigest()

executor = ThreadPoolExecutor()

def runMapper(exec_file, input_file, num_reducers):
    f = open(input_file, "r", encoding="utf-8")
    proc = subprocess.Popen([exec_file], stdin=f,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    result = [[] for i in range(0, num_reducers)]

    count = 0
    for line in proc.stdout:
        kv = line.decode("utf-8").split("\t")
        rid = hashf(kv[0]) % num_reducers
        result[rid].append([kv[0], kv[1]])
        count += 1

    for line in proc.stderr:
        print("map error:\t", line)

    # close file handlers
    f.close()
    proc.stdout.close()
    proc.stderr.close()

    ret = proc.wait()

    if ret != 0:
        return ret, {}

    for r in result:
        r.sort(key=lambda x: x[0])

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

def gen_map_requests(rix, cli, ids):
    worker_urls = utils.worker_urls()
    n_workers = manifest.WORKER_NUM
    urls = ["%s/retrive_map_output?reducer_ix=%d&map_task_id=%s" %
            (worker_urls[i % n_workers], rix, _id) for i, _id in enumerate(ids)]

    return [cli.fetch(url, connect_timeout=600, request_timeout=600) for url in urls]

from functools import reduce

def runReducer(exec_file, input_data, job_path, reducer_ix):
    output_file = os.path.join(job_path, "%d.out" % reducer_ix)
    err_file = os.path.join(job_path, "%d.err" % reducer_ix)
    f = open(output_file, "wb")
    ef = open(output_file, "wb")
    proc = subprocess.Popen([exec_file], stdin=subprocess.PIPE, stdout=f, stderr=ef)

    proc.stdin.write(input_data)
    proc.stdin.close()

    ret = proc.wait()

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
            job_path = self.get_argument("job_path")
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
            map_res.extend(json.loads(r.body))

        map_res.sort(key=lambda x: x[0])
        input_data = "\n".join(map(lambda x: "\t".join(x).strip(), map_res)).encode("utf-8")

        ret = 0
        try:
            ret = yield executor.submit(runReducer, reducer_path, input_data, job_path, reducer_ix)
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

if __name__ == "__main__":
    import sys
    app = make_worker_app()
    port = 8080 if len(sys.argv) == 1 else sys.argv[1]
    app.listen(port)
    IOLoop.current().start()