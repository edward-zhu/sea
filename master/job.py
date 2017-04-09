'''
job.py

Job class
'''

import hashlib
import time
import random

from collections import deque
from tornado.gen import coroutine

import yaml

'''

Job spec format (in yaml)

name : [job name]
tasks :
  - spec : spec1
    arg1 : value1
    arg2 : value2

  - spec : spec2
    arg1 : value1
    ...
'''

SALT = "serval"

def gen_tid(task_spec):
    '''generate a unique task id'''
    return hashlib.md5((task_spec + str(time.time()) + str(random.random()) + SALT)
                       .encode("utf-8")).hexdigest()

class _Task:
    '''Internal task representation'''
    SALT = "sdsf"

    STANDBY = 0
    RUNNING = 1
    FAILED = 2
    DONE = 3

    STATES = ["STANDBY", "RUNNING", "FAILED", "DONE"]

    def __init__(self, _tid, _spec, _args):
        self._tid = _tid
        self._spec = _spec # task spec
        self._args = _args # task args
        self._state = _Task.STANDBY
        self._worker = None
        self._err = ""

    @property
    def tid(self):
        '''my tid'''
        return self._tid

    @property
    def spec(self):
        '''my spec'''
        return self._spec

    @property
    def state(self):
        '''my state'''
        return self._state

    @property
    def state_str(self):
        '''my state in str'''
        return _Task.STATES[self._state]

    @state.setter
    def state(self, _state):
        '''change my state'''
        self._state = _state

    @property
    def worker(self):
        '''my worker'''
        return self._worker

    @worker.setter
    def worker(self, _w):
        '''set worker'''
        self._worker = _w

    @state.setter
    def state(self, _worker):
        '''assign my worker'''
        self._worker = _worker

    def finished(self):
        '''am I finished?'''
        return self.state == _Task.DONE or self.state == _Task.FAILED

    @property
    def err(self):
        '''my error'''
        return self._err

    @state.setter
    def state(self, _state):
        '''set state'''
        self._state = _state

    def fail(self, err):
        '''set fail'''
        self._err = err
        self._state = _Task.FAILED

    def new_task_req(self):
        '''get req string for creating new task.'''
        args = ["%s=%s" % (arg, self._args[arg]) for arg in self._args]
        return "/new/%s?tid=%s&%s" % (self.spec, self.tid, "&".join(args))

    def get_state_req(self):
        '''get req string for get task state'''
        return "/task/%s" % (self._tid, )

    def desc(self):
        '''my description'''
        return {
            "tid" : self.tid,
            "spec" : self.spec,
            "state" : self.state_str,
            "worker" : self.worker,
            "args" : self._args,
            "error" : self.err
        }

class Job:
    '''
    Job class

    Parse job spec from yaml file.
    '''

    STANDBY = 0
    RUNNING = 1
    DONE = 2

    STATES = ["STANDBY", "RUNNING", "DONE"]

    def __init__(self, tracker, jid, spec_fp):
        '''initialize a job from a yaml spec fp (.read())'''
        spec = yaml.load(spec_fp)
        self._tracker = tracker
        self._jid = jid
        self._name = spec["name"]
        self._tasks = {}
        self._state = Job.STANDBY
        for task in spec["tasks"]:
            spec = task["spec"]
            tid = gen_tid(spec)
            args = task
            args.pop("spec")
            self._tasks[tid] = _Task(tid, spec, args)
        self._waitqueue = deque([x for x in self._tasks])
        self._start_time = time.time()
        self._stop_time = None

    @property
    def jid(self):
        '''my id'''
        return self._jid

    @property
    def name(self):
        '''my name'''
        return self._name

    @property
    def state_str(self):
        '''my state in str'''
        return Job.STATES[self._state]

    def finished(self):
        '''if job is finished'''
        if len(self._waitqueue) > 0:
            return False

        for tid in self._tasks:
            if not self._tasks[tid].finished():
                return False
        
        return True

    def update_task(self, tid, state, err=""):
        '''update task state (from self and tracker)'''
        if tid not in self._tasks:
            return
        task = self._tasks[tid]

        print('job %s: task %s state update to: %s' % (self.jid, tid, state))

        if state == 'RUNNING':
            task.state = _Task.RUNNING
            return

        if state == 'FAILED':
            task.fail(err)
        elif state == 'DONE':
            task.state = _Task.DONE

        self._tracker.return_worker(task.worker)

        if self.finished():
            print('job %s: finished' % (self.jid, ))
            self._state = Job.DONE
            self._tracker.job_finish_received(self.jid)

    @coroutine
    def schedule(self):
        '''
        schedule next task (if there is one) with given worker

        return True if the worker is running with current task,
        otherwise, return False if the worker is not used (because
        task failed immediately.)
        '''
        print('job: schedule job.')

        if len(self._waitqueue) == 0:
            return

        worker = self._tracker.borrow_worker()
        if worker is None: # borrow worker failed
            return

        print('job: borrow worker.')

        tid = self._waitqueue.popleft()
        worker.set_owner(self.jid, tid)

        self._state = Job.RUNNING

        task = self._tasks[tid]
        task.worker = worker.host
        ret = yield worker.send(task.new_task_req())

        # if create task failed
        if ret["status"] != "ok":
            self.update_task(tid, 'FAILED', ret["error"])
            self._tracker.return_worker(task.worker)
            return

        self.update_task(tid, 'RUNNING')

    def desc(self):
        '''my description'''
        return {
            "jid" : self.jid,
            "name" : self.name,
            "state" : self.state_str,
            "tasks" : [t.desc() for _, t in self._tasks.items()]
        }
