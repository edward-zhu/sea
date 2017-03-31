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
        urls = str(args["urls"][0], encoding='utf-8').split(',')
        job_path = str(args["job_path"][0], encoding='utf-8')
        n_part = int(str(args["n_part"][0], encoding='utf-8'))
        print(urls, job_path, n_part)
        rfmt = Reformatter(urls, job_path, n_part)
        ioloop = IOLoop.current()
        ioloop.add_callback(self._run, rfmt)
