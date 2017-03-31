#!/usr/bin/env python
# encoding: utf-8

from mapreduce import manifest

WORKER_URLS = None

def _fnv32a(s):
    hval = 0x811c9dc5
    fnv_32_prime = 0x01000193
    uint32_max = 2 ** 32
    for c in s:
        hval = hval ^ ord(c)
        hval = (hval * fnv_32_prime) % uint32_max
    return hval

def hashf(s):
    return _fnv32a(s)

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

    params = ["%s=%s" % (k, v) for k, v in kwargs.items()]

    return "%s/%s?%s" % (base, method, "&".join(params))
