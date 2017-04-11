'''
task_tracker.py

TaskTracker class
'''

import hashlib
import time
import random
import traceback
import socket

import urllib.parse

from tornado.web import RequestHandler, Application
from tornado.queues import Queue
from tornado.gen import coroutine, sleep
from tornado.httpclient import AsyncHTTPClient
from tornado.ioloop import IOLoop, PeriodicCallback

from client.reformat_task import ReformatTask
from client.mapreduce_task import MapreduceTask
from client.task import Task


def _parse_args(_task, raw_args):
    args = {}
    print(raw_args)
    for arg in _task.args():
        # when the encoding is not utf-8 will raise UnicodeDecodeError
        args[arg] = str(raw_args[arg][0], 'utf-8')
    return args

class TaskTracker:
    '''
    Run new task and track tasks' running state.
    At any time, there can only be one running task.
    '''

    HEARTBEAT_INT = 2000 # heartbeat timeout in msec.
    TRUNCATED_SEC = 30 # truncated to seconds

    def __init__(self, job_tracker, task_specs):
        self.job_tracker = job_tracker
        self.task_specs = task_specs
        self._tasks = {}
        self.cur_task = None
        self.msgq = Queue()
        self.hbt_timer = None
        self._ioloop = None

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
    def _send_req(self, req, retry=False):
        http_cli = AsyncHTTPClient()
        # truncated exponential backoff
        delay = 1
        while True:
            try:
                print("req: ", self.job_tracker + req)
                ret = yield http_cli.fetch(self.job_tracker + req)
            except Exception as e:
                if retry and delay < TaskTracker.TRUNCATED_SEC:
                    print("send req failed: %s, retry after %d sec." % (str(e), delay,))
                    yield sleep(delay)
                    delay *= 2
                    continue
                else:
                    print("send req failed.")
                    return False, ""
            return True, ret

    @coroutine
    def _report_task_update(self, tid, state, err):
        yield self._send_req('/update/%s?state=%s&error="%s"' %
                             (tid, state, urllib.parse.quote(err)), True)


    @coroutine
    def msg_handler(self):
        while True:
            msg = yield self.msgq.get()
            yield self._report_task_update(msg["tid"], msg["state"], msg["error"])

    @coroutine
    def heartbeat(self):
        ok, res = yield self._send_req("/heartbeat?host=" + self.host)
        if not ok:
            print('Warning: connect to job tracker failed.')

        print(self.tasks())

    def _task_desc(self, task):
        TIME_FMT = "%Y-%m-%d %H:%M:%S"
        start = time.strftime(TIME_FMT, time.localtime(task.start_time))
        if task.finish_time is None:
            finish = "Pending"
        else:
            finish = time.strftime(TIME_FMT, time.localtime(task.finish_time))

        return {
            "tid": task.tid,
            "state" : task.state_str(),
            "start_time" : start,
            "finish_time" : finish,
        }

    def tasks(self):
        '''get all tasks and their state'''
        return [self._task_desc(self._tasks[tid]) for tid in self._tasks]

    def task(self, tid):
        if tid not in self._tasks:
            return None

        _t = self._tasks[tid]
        if _t.state == Task.FAILED:
            return _t.state_str(), _t.err

        return _t.state_str(), ""

    def run_task(self, tid, spec, args):
        '''
        run a task if this tracker is idle.

        the arguments of the task will be converted from raw
        bytes to python **str** with one item for each argument.
        '''
        if self.busy():
            return False, "Busy"
        if spec not in self.task_specs:
            return False, "No such task spec."

        task = self.task_specs[spec](self.msgq, tid)
        try:
            args = _parse_args(task, args)
            task.run(args)
        except KeyError as e:
            return False, "Missing argument %s" % str(e)
        except Exception as e:
            print(traceback.print_exc())
            return False, str(e)
        self._tasks[tid] = task
        self.cur_task = tid

        return True, ""

    def setup(self, host):
        self._ioloop = IOLoop.current()
        self._ioloop.add_callback(self.msg_handler)
        self.hbt_timer = PeriodicCallback(self.heartbeat, TaskTracker.HEARTBEAT_INT)
        self.hbt_timer.start()
        self.host = host
        print('task tracker setup: %s.' % (self.host,))


tracker = None

class NewTaskHandler(RequestHandler):
    def get(self, spec):
        args = self.request.arguments
        tid = self.get_argument("tid")
        success, err = tracker.run_task(tid, spec, args)
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

def get_internal_ip():
    return socket.gethostbyname(socket.gethostname())

def make_tracker_app(port, job_tracker):
    global tracker
    tracker = TaskTracker(job_tracker, {
        'reformat' : ReformatTask,
        'mapreduce' : MapreduceTask,
    })
    app = Application([
        (r'/status', GetStatusHandler),
        (r'/task/([^/]+)', GetTaskHandler),
        (r'/new/([^/]+)', NewTaskHandler),
    ])
    tracker.setup("http://%s:%d" % (get_internal_ip(), port, ))

    return app

if __name__ == '__main__':
    import os

    if "JOB_TRACKER" not in os.environ:
        print("can not find settings in env, exit.")
        exit(1)

    port = 8800 if "TASK_TRACKER_PORT" not in os.environ else int(os.environ["TASK_TRACKER_PORT"])
    job_tracker = os.environ["JOB_TRACKER"]

    app = make_tracker_app(port, job_tracker)
    app.listen(port)
    print('new task tracker listening on %d.' % (port,))
    print('using job tracker: %s.' % (job_tracker,))
    IOLoop.current().start()
