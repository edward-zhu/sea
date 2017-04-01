'''
index_task.py

orchestrate index process in each node.
'''

from tornado.ioloop import IOLoop
from tornado.gen import coroutine

from client.reformatter import Reformatter
from client.task import Task

'''
Tasks:
- copy all assigned raw file to local
- run inverted-index mapreduce job
'''

class ReformatTask(Task):
    '''Call Reformatter to reformat raw documents'''
    @coroutine
    def _run(self, rfmt):
        self.set_running()
        ret, err = yield rfmt.run()
        if ret:
            self.set_done()
            return
        self.set_failed(err)

    def run(self, args):
        urls = args["urls"].split(',')
        job_path = args["job_path"]
        n_part = int(args["n_part"])
        n_group = int(args["n_group"])
        start_gid = int(args["start_gid"])
        print(urls, job_path, n_part)
        rfmt = Reformatter(urls, job_path, n_part, n_group, start_gid)
        ioloop = IOLoop.current()
        ioloop.add_callback(self._run, rfmt)

    def args(self):
        return ["urls", "job_path", "n_part", "n_group", "start_gid"]
