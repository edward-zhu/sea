#!/usr/bin/env python
# encoding: utf-8

import os
import argparse
import json

from tornado.ioloop import IOLoop
from tornado.httpclient import AsyncHTTPClient
from tornado.gen import coroutine

from mapreduce import manifest
from mapreduce import utils


def parse_args():
    '''parse arguments'''
    parser = argparse.ArgumentParser()
    parser.add_argument("--mapper_path", required=True, help="mapper .py's path")
    parser.add_argument("--reducer_path", required=True, help="reducer .py's path")
    #parser.add_argument("--job_path", required=True, help="path to input data files.")
    parser.add_argument("--input_path", required=True, help="path to input data files.") #divide the input and output
    parser.add_argument("--output_path", required=True, help="path to output data files.")
    parser.add_argument("--num_reducers", type=int, required=True, help="number of reducers")

    return parser.parse_args()

class Coordinator:
    def __init__(self, mapper_path, reducer_path, input_path, output_path, num_reducers):
        AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")
        self.mapper_path = mapper_path
        self.reducer_path = reducer_path
        #self.job_path = job_path
        self.input_path = input_path
        self.output_path = output_path
        self.num_reducers = num_reducers

    def err(self, phase, err):
        return "fatal: at %s phase: %s" % (phase, err)

    def _get_input_files(self):
        files = []
        for path in self.input_path:
            # fs = list(filter(lambda x: x[-3:] in [".in", ".in.bz2"], os.listdir(path)))
            fs = os.listdir(path.strip())
            fs = map(lambda x: os.path.join(path, x), fs)
            files.extend(fs)
            #files = os.listdir(self.input_path)
        return files

    def _get_map_reqs(self, cli, files):
        worker_urls = utils.worker_urls()
        urls = [utils.gen_req_url(worker_urls[i % manifest.WORKER_NUM], "map",
                                  mapper_path=self.mapper_path,
                                  input_file=f,
                                  num_reducers=self.num_reducers) for i, f in enumerate(files)]
        print(urls)
        return [cli.fetch(url, request_timeout=6000) for url in urls]

    def _get_reduce_reqs(self, cli, map_task_ids):
        worker_urls = utils.worker_urls()
        urls = [utils.gen_req_url(worker_urls[i], "reduce",
                                  reducer_ix=i,
                                  reducer_path=self.reducer_path,
                                  map_task_ids=map_task_ids,
                                  input_path=self.input_path,
                                  output_path=self.output_path)
                for i in range(0, self.num_reducers)]
        return [cli.fetch(url, request_timeout=6000) for url in urls]

    @coroutine
    def run(self):
        input_files = self._get_input_files()

        # Map phase
        map_cli = AsyncHTTPClient()
        map_res = yield self._get_map_reqs(map_cli, input_files)
        map_task_ids = []
        for res in map_res:
            if res.code != 200:
                return self.err("map", "connection failed: %d" % res.code)
            data = json.loads(str(res.body, encoding="utf-8"))
            if data["status"] != "success":
                if "error" in data:
                    return self.err("map", "mapper failed: %s" % data["error"])
                else:
                    return self.err("map", "mapper failed unexpectedly")
            map_task_ids.append(data["map_task_id"])
        print("map phase done.")

        # Reduce phase
        reduce_cli = AsyncHTTPClient()
        reduce_res = yield self._get_reduce_reqs(reduce_cli, map_task_ids)
        for res in reduce_res:
            if res.code != 200:
                return self.err("reduce", "connection failed %d" % res.code)
            data = json.loads(str(res.body, encoding="utf-8"))
            if data["status"] != "success":
                if "error" in data:
                    return self.err("reduce", "reducer failed: %s" % data["error"])
                else:
                    return self.err("reduce", "reducer failed unexpectedly")
        print("reduce phase done.")

        return "ok"

    def run_sync(self):
        '''run coordinator in a sync way (wait until the mapreduce job finished.)'''
        print("start coordinator..")
        return IOLoop().run_sync(self.run)

@coroutine
def main(args):
    '''main func'''
    paths = [p.strip() for p in args.input_path.split(',')]
    c = Coordinator(
        args.mapper_path,
        args.reducer_path,
        paths,
        args.output_path,
        args.num_reducers)
    err = yield c.run()
    print(err)
    IOLoop.instance().stop()


if __name__ == "__main__":
    ioloop = IOLoop.instance()
    ioloop.add_callback(main, parse_args())
    ioloop.start()

