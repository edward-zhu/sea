#!/usr/bin/env python
# encoding: utf-8

'''
workers.py

Using circus to spawn all the worker process
'''

import os
import signal
import sys
import subprocess

from mapreduce import worker, utils, manifest



apps = []

def sigterm_hdl(signo, stack):
    map(lambda app: app.kill(), apps)
    sys.exit(0)

if __name__ == '__main__':
    env = os.environ.copy()
    signal.signal(signal.SIGTERM, sigterm_hdl)
    signal.signal(signal.SIGTSTP, sigterm_hdl)
    signal.signal(signal.SIGINT, sigterm_hdl)
    for i in range(0, manifest.WORKER_NUM):
        port = manifest.BASE_PORT + i
        proc = subprocess.Popen(args=["python", "-u", "-m", "mapreduce.worker", str(port)],
                                stdout=sys.stdout,
                                stderr=sys.stderr,
                                env=env)
        apps.append(proc)

    try:
        for app in apps:
            app.wait()
    finally:
        print("stoped!")
        map(lambda app: app.kill(), apps)
