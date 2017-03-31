'''
task_tracker.py

TaskTracker class
'''

import hashlib
import time
import random

from tornado.web import RequestHandler, Application
from tornado.queues import Queue
from tornado.gen import coroutine
from tornado.ioloop import IOLoop, PeriodicCallback

from reformat_task import ReformatTask
from task import Task

SALT = "sdsf"

def gen_tid(task_spec):
    '''generate a unique task id'''
    return hashlib.md5((task_spec + str(time.time()) + str(random.random()) + SALT)
                       .encode("utf-8")).hexdigest()

class TaskTracker:
    '''
    Run new task and track tasks' running state.
    At any time, there can only be one running task.
    '''

    HEARTBEAT_INT = 2000

    def __init__(self, job_tracker, task_specs):
        self.job_tracker = job_tracker
        self.task_specs = task_specs
        self._tasks = {}
        self.cur_task = None
        self.msgq = Queue()
        self.hbt_timer = None

    def current(self):
        '''return the current or most-recent task'''
        if self.cur_task is None:
            return None
        return self._tasks[self.cur_task]

    def busy(self):
        '''is there anyone running?'''
        cur = self.current()
        if cur is None or cur.stopped():
            return False

    @coroutine
    def msg_handler(self):
        while True:
            msg = yield self.msgq.get()
            # TODO: trigger
            print(msg)

    def heartbeat(self):
        # TODO: send heartbeat to job tracker
        print(self.tasks())

    def tasks(self):
        '''get all tasks and their state'''
        return [(tid, self._tasks[tid].state()) for tid in self._tasks]

    def task(self, tid):
        if tid not in self._tasks:
            return None

        _t = self._tasks[tid]
        if _t.state() == Task.FAILED:
            return _t.state_str(), _t.err

        return _t.state_str(), ""

    def run_task(self, spec, args):
        '''run a task if this tracker is idle.'''
        if self.busy():
            return False, "", "Busy"
        if spec not in self.task_specs:
            return False, "", "No such task spec."

        tid = gen_tid(spec)
        task = self.task_specs[spec](self.msgq, tid)
        try:
            task.run(args)
        except Exception as e:
            return False, "", str(e)
        self._tasks[tid] = task
        self.cur_task = tid

        return True, tid, ""

    def setup(self):
        ioloop = IOLoop.current()
        ioloop.add_callback(self.msg_handler)
        self.hbt_timer = PeriodicCallback(self.heartbeat, TaskTracker.HEARTBEAT_INT)
        self.hbt_timer.start()

tracker = None

class NewTaskHandler(RequestHandler):
    def get(self, spec):
        args = self.request.arguments
        success, tid, err = tracker.run_task(spec, args)
        if success:
            self.write({'status': 'ok', 'tid' : tid})
            return

        self.write({'status' : 'error', 'error' : err})


class GetStatusHandler(RequestHandler):
    def get(self):
        self.write({'status' : 'ok', 'busy': tracker.busy(), 'tasks' : tracker.tasks()})

class GetTaskHandler(RequestHandler):
    def get(self, tid):
        state, err = tracker.task(tid)
        if state == None:
            self.set_status(404)
            return
        self.write({'status' : 'ok', 'state' : state, 'error' : err})

def makeTrackerApp():
    global tracker
    tracker = TaskTracker("", {
        'reformat' : ReformatTask,
    })
    app = Application([
        (r'/status', GetStatusHandler),
        (r'/task/([^/]+)', GetTaskHandler),
        (r'/new/([^/]+)', NewTaskHandler),
    ])
    tracker.setup()

    return app

if __name__ == '__main__':
    app = makeTrackerApp()
    app.listen(8800)
    print('listening on 8800.')
    IOLoop.current().start()
