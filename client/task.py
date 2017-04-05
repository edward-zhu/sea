'''
task.py

Abstract Task class.
'''

import time
from tornado.queues import Queue

class Task:
    '''
    Abstract Task

    inherited class should implement *run* and *args* methods.
    '''

    STANDBY = 0
    RUNNING = 1
    FAILED = 2
    DONE = 3

    STATES = ['STANDBY', 'RUNNING', 'FAILED', 'DONE']

    def __init__(self, q, tid):
        self._state = Task.STANDBY
        self.msgq = q # msg queue to coordinator
        self._tid = tid
        self.err = ""
        self._start_time = time.time()
        self._finish_time = None
        assert isinstance(self.msgq, Queue)

    def set_state(self, state):
        '''set broker's running state'''
        self._state = state
        if state in [Task.FAILED, Task.DONE]:
            self._finish_time = time.time()
        self.msgq.put({"tid" : self.tid, "state" : self.state_str(), "error" : self.err})

    def set_running(self):
        '''set to running state'''
        self.set_state(Task.RUNNING)

    def set_done(self):
        '''set to done state'''
        self.set_state(Task.DONE)

    def set_failed(self, err):
        '''set to failed state'''
        self.err = err
        print("task failed:", err)
        self.set_state(Task.FAILED)

    # task.start_time
    # task.start_time() X
    # task.start_time = sdfsdfds X

    @property
    def start_time(self):
        '''get task's start time'''
        return self._start_time

    @property
    def finish_time(self):
        '''get task's finish time'''
        return self._finish_time

    @property
    def tid(self):
        '''get tid'''
        return self._tid

    @property
    def state(self):
        '''get current state'''
        return self._state

    def state_str(self):
        '''task's state in string'''
        return Task.STATES[self.state]

    def stopped(self):
        '''is the task stopped?'''
        _state = self.state
        return _state != Task.STANDBY and _state != Task.RUNNING

    @property
    def name(self):
        '''Task's name'''
        return self.__class__.__name__

    def run(self, args):
        '''run task (async)'''
        raise NotImplementedError

    def args(self):
        '''returns all required arguments' name in string array'''
        raise NotImplementedError

    def __str__(self):
        '''Task instance's description'''
        return "%s:%d [%s]" % (self.name, self.tid, self.state_str())
