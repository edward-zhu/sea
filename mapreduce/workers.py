#!/usr/bin/env python
# encoding: utf-8

'''
workers.py

One thread for all workers.
'''

from tornado.ioloop import IOLoop
from tornado.web import Application

from mapreduce import worker, utils, manifest

if __name__ == '__main__':
    apps = []
    for i in range(0, manifest.WORKER_NUM):
        apps.append(worker.make_worker_app())
        port = manifest.BASE_PORT + i
        apps[i].listen(port)
        print("worker %d listen on %s" % (i, utils.worker_url(i)))

    IOLoop.current().start()
