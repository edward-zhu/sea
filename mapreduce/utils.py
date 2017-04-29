#!/usr/bin/env python
# encoding: utf-8

import common

from mapreduce import manifest
from urllib.parse import quote


WORKER_URLS = None

hashf = common.hashf

def worker_url(i):
    return worker_urls()[i]

def worker_urls():
    global WORKER_URLS
    if WORKER_URLS is None:
        WORKER_URLS = [_worker_url(i) for i in range(0, manifest.WORKER_NUM)]

    return WORKER_URLS

def _worker_url(i):
    if i < 0 or i >= manifest.WORKER_NUM:
        raise KeyError("no such worker.")

    return "http://%s:%d" % (manifest.HOST, manifest.BASE_PORT + i)

def gen_req_url(base, method, **kwargs):
    for k in kwargs:
        if type(kwargs[k]) is list:
            stred = [str(v) for v in kwargs[k]]
            kwargs[k] = ",".join(stred)
        elif type(kwargs[k]) in [int, str, float]:
            kwargs[k] = str(kwargs[k])
        else:
            raise TypeError("Unsupport type:" + str(type(kwargs[k])))

    params = ["%s=%s" % (k, quote(v)) for k, v in kwargs.items()]

    return "%s/%s?%s" % (base, method, "&".join(params))
