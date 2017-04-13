#!/usr/bin/env python
# encoding: utf-8

'''
workers.py

Using circus to spawn all the worker process
'''

import os
import time
import signal
import sys
import subprocess

from mapreduce import worker, utils, manifest

apps = []

def sigterm_hdl(signo, stack):
    print("got stop signal %d" % (signo, ))
    for app in apps:
        print("send SIGTERM to pid %d" % (app.pid))
        app.terminate()
    map(lambda app: app.stdout.close, apps)
    map(lambda app: app.stderr.close, apps)

    time.sleep(1)

    print("closed")

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
        print("proc: %d" % (proc.pid, ))
        apps.append(proc)

    try:
        for app in apps:
            app.wait()
    finally:
        print("program exit!")
        sys.exit(0)

