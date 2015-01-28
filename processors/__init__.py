# coding=utf-8
from Queue import Empty
import signal

from core import LoggerMixin


__author__ = 'zephyre'

import gevent


def runproc(func):
    def wrapper(self):
        import sys

        self.log('Processor started: %s' % ' '.join(sys.argv))

        self._start_workers()
        func(self)
        self._join()

    return wrapper


class BaseProcessor(LoggerMixin):
    name = 'base-processor'

    @classmethod
    def from_engine(cls, engine, *args, **kwargs):
        return cls(engine, *args, **kwargs)

    def _request(self):
        return self.engine.request

    request = property(_request)

    def __init__(self, engine, *args, **kwargs):
        from time import time
        from hashlib import md5
        from gevent.queue import Queue

        self.processor_name = '%s:%s' % (self.name, md5(str(time())).hexdigest()[:6])

        LoggerMixin.__init__(self)

        self.engine = engine

        self.progress = 0
        self.total = 0
        self.maxsize = 1000
        self.op_done = False
        self.tasks = Queue(self.maxsize)
        self.jobs = []

        self.arg_parser = self.engine.arg_parser
        self.arg_parser.add_argument('--concur', default=10, type=int)
        ret, leftover = self.arg_parser.parse_known_args()
        self.args = ret
        self.concur = ret.concur

        self.checkpoint_ts = None
        self.checkpoint_prog = None
        self.init_ts = time()

    def _join(self):
        self.op_done = True
        gevent.joinall(self.jobs[1:])
        gevent.kill(self.jobs[0])

    def _start_workers(self):
        def worker():
            while True:
                if self.op_done and self.tasks.empty():
                    break
                try:
                    task = self.tasks.get(timeout=1)
                except Empty:
                    continue

                try:
                    task()
                except Exception as e:
                    if e.message:
                        self.logger.error('Error occured: %s' % e.message, exc_info=True)
                    else:
                        self.logger.error('Error occured: unknown', exc_info=True)

                gevent.sleep(0)

        def timer():
            import time

            while not self.tasks.empty() or not self.op_done:
                msg = 'Progress: %d / %d.' % (self.progress, self.total)

                cts = time.time()

                if self.checkpoint_prog is not None and self.checkpoint_ts is not None:
                    rate = (self.progress - self.checkpoint_prog) / (cts - self.checkpoint_ts) * 60
                    msg = '%s %s' % (msg, 'Processing rate: %d items/min' % int(rate))

                self.checkpoint_ts = cts
                self.checkpoint_prog = self.progress

                self.log(msg)
                gevent.sleep(30)

        gevent.signal(signal.SIGKILL, gevent.kill)
        gevent.signal(signal.SIGQUIT, gevent.kill)
        self.jobs = [gevent.spawn(timer)]
        for i in xrange(self.concur):
            self.jobs.append(gevent.spawn(worker))

    def add_task(self, task):

        def wrapper():
            self.progress += 1
            task()

        self.tasks.put(wrapper, timeout=120)
        gevent.sleep(0)

    def run(self):
        import time

        self._start_workers()

        self.populate_tasks()

        self._join()

        self.log('Processor ended. %d items processed in %d minutes' % (self.progress,
                                                                        int((time.time() - self.init_ts) / 60.0)))

    def populate_tasks(self):
        raise NotImplementedError
