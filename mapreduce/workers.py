#!/usr/bin/env python
# encoding: utf-8

'''
workers.py

Using circus to spawn all the worker process
'''

import os
from circus import get_arbiter

from mapreduce import worker, utils, manifest

if __name__ == '__main__':
    apps = []
    for i in range(0, manifest.WORKER_NUM):
        port = manifest.BASE_PORT + i
        apps.append({
            "cmd": "python",
            "args" : "-u -m mapreduce.worker %d" % (port, ),
            "env" : os.environ.copy(),})

    arbiter = get_arbiter(apps)

    try:
        arbiter.start()
    finally:
        arbiter.stop()
        #print("worker %d listen on %s" % (i, utils.worker_url(i)))
