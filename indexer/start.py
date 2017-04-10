#pylint:disable=C0103
'''
start.py

Start full mapreduce jobs for indexing locally.
'''

import os

from mapreduce.coordinator import Coordinator
from search.manifest import N_DOC_SRV, N_INDEX_SRV
from indexer.integrate import integrate

from tornado.ioloop import IOLoop
from tornado.gen import coroutine

def mr_app_path(fn):
    '''generate file path for mapreduce apps'''
    return "indexer/mr_apps/" + fn

@coroutine
def run_mapreduce(mapper, reducer, job_path, num_reducers):
    '''run one mapreduce job'''
    c = Coordinator(mapper, reducer, job_path, num_reducers)
    ret = yield c.run()
    return ret


@coroutine
def gen_inverted_index():
    mapper = mr_app_path("invindex_mapper.py")
    reducer = mr_app_path("invindex_reducer.py")
    job_path = "assignment4/invindex_jobs"
    num_reducers = N_INDEX_SRV
    ret = yield run_mapreduce(mapper, reducer, job_path, num_reducers)

    return ret

@coroutine
def gen_idf():
    mapper = mr_app_path("idf_mapper.py")
    reducer = mr_app_path("idf_mapper.py")
    job_path = "assignment4/idf_jobs"
    num_reducers = 1
    ret = yield run_mapreduce(mapper, reducer, job_path, num_reducers)

    return ret

@coroutine
def gen_docs():
    mapper = mr_app_path("docs_mapper.py")
    reducer = mr_app_path("docs_reducer.py")
    job_path = "assignment4/docs_jobs"
    num_reducers = N_DOC_SRV
    ret = yield run_mapreduce(mapper, reducer, job_path, num_reducers)

    return ret

def move(src, src_pat, dst, dst_pat, num):
    for i in range(0, num):
        src_path = os.path.join(src, src_pat % (i,))
        dst_path = os.path.join(dst, dst_pat % (i,))
        os.rename(src_path, dst_path)

@coroutine
def main():
    '''main func'''
    ret = yield gen_inverted_index()
    print(ret)
    move("assignment4/invindex_jobs",
         "%d.out",
         "assignment4/idf_jobs",
         "%d.in", N_INDEX_SRV)

    ret = yield gen_idf()
    print(ret)
    move("assignment4/invindex_jobs",
         "reformatted_%d.in",
         "assignment4/docs_jobs",
         "%d.in", N_INDEX_SRV)
    ret = yield gen_docs()
    print(ret)
    IOLoop.instance().stop()
    integrate("assignment4/docs_jobs/%d.out",
              "assignment4/idf_jobs/%d.in",
              "assignment2/data",
              "docs",
              "indexes",
              N_DOC_SRV)

if __name__ == "__main__":
    ioloop = IOLoop.instance()
    ioloop.add_callback(main)
    ioloop.start()
    