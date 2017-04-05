'''
job_tracker.py

JobTracker class
'''

import hashlib
import time
import random
import json

from tornado.web import RequestHandler, Application
from tornado.httpclient import AsyncHTTPClient
from tornado.gen import coroutine
from tornado.ioloop import IOLoop

from master.job import Job


SALT = "servalchan"

def gen_jid():
    '''generate a unique job id'''
    return hashlib.md5((str(time.time()) + str(random.random()) + SALT)
                       .encode("utf-8")).hexdigest()

class Worker:
    IDLE = 0
    BUSY = 1
    DEAD = 2
    LOST = 3

    def __init__(self, host):
        self._host = host
        self._state = Worker.IDLE
        self._lasthbt = time.time()
        self._httpcli = AsyncHTTPClient()
        self._jid = "None"
        self._tid = "None"

    def set_owner(self, jid, tid):
        self._jid = jid
        self._tid = tid

    def owner(self):
        return self._jid, self._tid

    @property
    def host(self):
        '''my hostname'''
        return self._host

    @property
    def state(self):
        '''my state'''
        return self._state

    @state.setter
    def state(self, _state):
        self._state = _state

    def update_hbt(self):
        self._lasthbt = time.time()

    def since_last_hbt(self):
        return time.time - self._lasthbt

    @coroutine
    def send(self, req):
        '''send request to worker'''
        ret = yield self._httpcli.fetch(self._host + req)
        return json.loads(ret.body)

    def desc(self):
        return {
            "host" : self._host,
            "state" : self._state,
            "last_hbt" : self._lasthbt,
            "owner_jid" : self._jid,
            "owner_tid" : self._tid,
        }

class JobTracker:
    '''
    JobTracker

    Task:
    - job = [task1, task2, ... taskN]
    - track task_tracker status
        - receive heartbeat from task_trackers
        - if it's a new task_tracker, add to the new available workers.
        - if there is a long time since the last heartbeat:
            - mark its task failed
            - mark the worker unavailable
    - receive new job request from clients
    - track task progress:
        - when task failed, assign another available worker for this task.
        - when task done, assign another unstarted job to this worker
    '''

    HBT_DEADLINE = 30 # heartbeat timeout in second

    def __init__(self):
        self._workers = {} # initially no worker is visible or available
        self._jobs = {}
        self._cur_job = None

    def check_heartbeat(self):
        # TODO: check heartbeat timeout
        pass

    @coroutine
    def add_worker(self, host):
        '''add a new worker'''
        w = Worker(host)
        self._workers[host] = w
        if self._cur_job is not None:
            yield self._cur_job.schedule()

    def borrow_worker(self):
        '''borrow one idle worker for a job'''
        workers = self.idle_workers()
        if len(workers) == 0:
            return None
        w = self._workers[workers[0]]
        w.state = Worker.BUSY
        return w

    def return_worker(self, host):
        '''job return ownership to tracker'''
        worker = self._workers[host]
        worker.set_owner("None", "None")
        worker.state = Worker.IDLE

    @coroutine
    def heartbeat_received(self, host):
        if host in self._workers:
            self._workers[host].update_hbt()
            return

        yield self.add_worker(host)

    def job_finish_received(self, jid):
        '''listen from Job class that current job is succeed'''
        print('job_tracker job %s finish' % jid)
        self._cur_job = None

    @coroutine
    def task_update_received(self, tid, state, err):
        if self._cur_job is None:
            return
        self._cur_job.update_task(tid, state, err)
        if self._cur_job is not None:
            yield self._cur_job.schedule()

    def job(self, jid):
        return self._jobs[jid]

    def idle_workers(self):
        idles = []
        for host in self._workers:
            if self._workers[host].state == Worker.IDLE:
                idles.append(host)
        return idles

    def desc(self):
        return {
            "workers" : [w.desc() for _, w in self._workers.items()],
            "current" : "None" if self._cur_job is None else self._cur_job.desc(),
            "jobs" : [j.desc() for _, j in self._jobs.items()],
        }

    @coroutine
    def run_job(self, fd):
        if self._cur_job is not None:
            return False, "BUSY"

        jid = gen_jid()
        job = Job(self, jid, fd)
        self._jobs[jid] = job
        self._cur_job = job

        yield self._cur_job.schedule()

        return True, ""

tracker = None

class HeartbeatReqHandler(RequestHandler):
    @coroutine
    def get(self):
        host = self.get_argument('host')
        yield tracker.heartbeat_received(host)
        self.write({"status" : "ok"})

class TaskUpdateRequest(RequestHandler):
    @coroutine
    def get(self, tid):
        state = self.get_argument('state')
        error = self.get_argument('error')
        yield tracker.task_update_received(tid, state, error)
        self.write({"status" : "ok"})

class JobCreateRequest(RequestHandler):
    @coroutine
    def get(self):
        job_spec = self.get_argument('job_spec')
        f = open(job_spec)
        ok, err = yield tracker.run_job(f)
        f.close()
        if ok:
            self.write({"status" : "ok"})
            return
        self.write({"status" : "error", "error" : err})

class GetStatusHandler(RequestHandler):
    @coroutine
    def get(self):
        self.write(json.dumps(tracker.desc(), indent=1))


def make_tracker_app():
    global tracker
    tracker = JobTracker()

    app = Application([
        (r'/heartbeat', HeartbeatReqHandler),
        (r'/update/([^/]+)', TaskUpdateRequest),
        (r'/new', JobCreateRequest),
        (r'/status', GetStatusHandler),
    ])

    return app

if __name__ == '__main__':
    app = make_tracker_app()
    app.listen(9000)
    print('job tracker listening on 9000.')
    IOLoop.current().start()




