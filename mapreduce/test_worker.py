#!/usr/bin/env python
# encoding: utf-8

from tornado.ioloop import IOLoop
from tornado.testing import AsyncTestCase, AsyncHTTPTestCase, gen_test

import json
from concurrent.futures import ProcessPoolExecutor

import manifest
import worker

def get_word_array(file):
    f = open(file, "r")
    words = []
    s = f.read()
    f.close()
    for l in s.split():
        words.append(l)
    words.sort()
    return words

executor = ProcessPoolExecutor()

class TestWorker(AsyncTestCase):
    @gen_test(timeout=100)
    def test_mapper(self):
        file = "fish_jobs/0.in"
        words = get_word_array(file)
        ret, res = yield executor.submit(worker.runMapper, "wordcount/mapper.py", file, 4)
        self.assertEqual(ret, 0)
        words2 = []
        for part in res:
            for k in part:
                words2.append(k[0])
        words2.sort()
        self.assertEqual(words, words2)

    @gen_test
    def test_reducer(self):
        test_str ='''abc\t1
abc\t1
fish\t1
'''
        print(test_str.encode("utf-8"))
        ret = yield executor.submit(worker.runReducer, "wordcount/reducer.py", test_str.encode("utf-8"), "fish_jobs", 0)
        self.assertEqual(ret, 0)

import os

class TestWorkerApp(AsyncHTTPTestCase):
    def get_app(self):
        return worker.make_worker_app()

    def test_map(self):
        manifest.BASE_PORT = self.get_http_port()
        job_path = "fish_jobs"
        file = "fish_jobs/2.in"
        n_reducers = 1
        response = self.fetch('/map?mapper_path=wordcount/mapper.py&input_file=%s&num_reducers=%d' % (file, n_reducers))
        self.assertEqual(response.code, 200)
        data = json.loads(response.body)
        self.assertEqual(data["status"], "success")
        task_id = data["map_task_id"]

        print(task_id)

        words = get_word_array(file)
        words2 = []
        for i in range(0, n_reducers):
            res = self.fetch('/retrive_map_output?reducer_ix=%d&map_task_id=%s' % (i, task_id))
            self.assertEqual(res.code, 200)
            data = json.loads(res.body.decode("utf-8"))
            for kv in data:
                words2.append(kv[0])

        words2.sort()

        print('reducer...')
        self.assertEqual(words, words2)
        for i in range(0, n_reducers):
            res = self.fetch(('/reduce?reducer_ix=%d&' +
                         'reducer_path=wordcount/reducer.py&' +
                         'map_task_ids=%s&job_path=%s') % (i, task_id, job_path))
            self.assertEqual(res.code, 200)
            f = open(os.path.join(job_path, "%d.out" % i))
            print(f.read())
            f.close()
