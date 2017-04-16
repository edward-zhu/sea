'''
index_task.py

orchestrate index process in each node.
'''

from tornado.ioloop import IOLoop
from tornado.gen import coroutine

from client.task import Task
from indexer import integrate

from concurrent.futures import ProcessPoolExecutor

'''
Tasks:
- copy all assigned raw file to local
- run inverted-index mapreduce job
'''

class IntegrateTask(Task):
    executor = ProcessPoolExecutor()

  

    @coroutine
    def _run(self):
        self.set_running()
        err = yield IntegrateTask.executor.submit(integrate.integrate, self.doc_result, self.inv_result,
                                                                      self.out_path, self.doc_prefix, self.inv_prefix,
                                                                      self.idf_path, self.nparts)
                                                                     
        if err == "ok":
            self.set_done()
            return
        self.set_failed(err)

    def run(self, args):
        self.doc_result = args["doc_result"]
        self.inv_result = args["inv_result"]
        self.out_path = args["out_path"]
        self.doc_prefix = args["doc_prefix"]
        self.inv_prefix = args["inv_prefix"]
        self.idf_path = args["idf_path"]
        self.nparts = int(args["nparts"])

        ioloop = IOLoop.current()
        ioloop.add_callback(self._run)

    def args(self):
        return ["doc_result", "inv_result", "out_path", "doc_prefix", "inv_prefix", "idf_path", "nparts"]