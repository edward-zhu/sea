#!/usr/bin/env python
# encoding: utf-8

'''
workers.py

Using circus to spawn all the worker process
'''

import os
import time
import sys
import subprocess

from mapreduce import worker, utils, manifest

if __name__ == '__main__':
    apps = []
    env = os.environ.copy()
    for i in range(0, manifest.WORKER_NUM):
        port = manifest.BASE_PORT + i
        proc = subprocess.Popen(args=["python", "-u", "-m", "mapreduce.worker", str(port)],
                                stdout=sys.stdout,
                                stderr=sys.stderr,
                                env=env,
                                encoding="utf-8")
        apps.append(proc)

    try:
        for app in apps:
            app.wait()
    finally:
        print("stoped!")
        map(lambda app: app.kill(), apps)
