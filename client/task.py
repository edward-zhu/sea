'''
task.py

Abstract Task class.
'''

from tornado.queues import Queue

class Task:
    '''Abstract Task'''

    STANDBY = 0
    RUNNING = 1
    FAILED = 2
    DONE = 3

    STATES = ['STANDBY', 'RUNNING', 'FAILED', 'DONE']

    def __init__(self, q, tid):
        self._state = Task.STANDBY
        self.msgq = q # msg queue to coordinator
        self.tid = tid
        self.err = ""
        assert isinstance(self.msgq, Queue)

    def set_state(self, state):
        '''set broker's running state'''
        self._state = state
        self.msgq.put({"id" : self.tid, "state" : self.state()})

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

    def state(self):
        '''get current state'''
        return self._state


    def state_str(self):
        '''task's state in string'''
        return Task.STATES[self.state()]

    def stopped(self):
        '''is the task stopped?'''
        _state = self.state()
        return _state != Task.STANDBY and _state != Task.RUNNING

    def run(self, args):
        '''run task (async)'''
        pass

    def name(self):
        '''Task's name'''
        pass

    def __str__(self):
        '''Task instance's description'''
        return "%s:%d [%s]" % (self.name(), self.tid, self.state_str())
