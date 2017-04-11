'''
index_task.py

orchestrate index process in each node.
'''

from tornado.ioloop import IOLoop
from tornado.gen import coroutine

from client.task import Task
from mapreduce.coordinator import Coordinator

from concurrent.futures import ProcessPoolExecutor

'''
Tasks:
- copy all assigned raw file to local
- run inverted-index mapreduce job
'''

class MapreduceTask(Task):
    executor = ProcessPoolExecutor()

    '''Call Coordinator to do mapreduce jobs'''
    @coroutine
    def _run(self, cdnt):
        self.set_running()
        err = yield MapreduceTask.executor.submit(cdnt.run_sync)
        if err == "ok":
            self.set_done()
            return
        self.set_failed(err)

    def run(self, args):
        mapper_path = args["mapper_path"]
        reducer_path = args["reducer_path"]
        input_path = args["input_path"].split(',')
        output_path = args["output_path"]
        num_reducers = int(args["num_reducers"])
        print(mapper_path, reducer_path, input_path, output_path, num_reducers)
        cdnt = Coordinator(mapper_path, reducer_path, input_path, output_path, num_reducers)
        ioloop = IOLoop.current()
        ioloop.add_callback(self._run, cdnt)

    def args(self):
        return ["mapper_path", "reducer_path", "input_path", "output_path", "num_reducers"]