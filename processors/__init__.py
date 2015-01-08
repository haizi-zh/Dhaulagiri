from Queue import Empty
import traceback
import signal

__author__ = 'zephyre'

import gevent


class BaseProcessor(object):
    def __init__(self):
        from gevent.queue import Queue

        self.progress = 0
        self.total = 0
        self.concur = BaseProcessor.args_builder().concur
        self.maxsize = 1000
        self.op_done = False
        self.tasks = Queue(self.maxsize)
        self.jobs = []

    @staticmethod
    def args_builder():
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument('--concur', default=10, type=int)
        args, leftovers = parser.parse_known_args()
        return args

    def join(self):
        self.op_done = True
        gevent.joinall(self.jobs[1:])
        gevent.kill(self.jobs[0])

    def run(self):
        def worker():
            while not self.tasks.empty() or not self.op_done:
                try:
                    task = self.tasks.get(timeout=1)
                except Empty:
                    continue

                try:
                    task()
                except Exception:
                    traceback.print_exc()

                gevent.sleep(0)

        def timer():
            while not self.tasks.empty() or not self.op_done:
                print 'Progress: %d / %d' % (self.progress, self.total)
                gevent.sleep(60)

        gevent.signal(signal.SIGKILL, gevent.kill)
        gevent.signal(signal.SIGQUIT, gevent.kill)
        self.jobs = [gevent.spawn(timer)]
        for i in xrange(self.concur):
            self.jobs.append(gevent.spawn(worker))

    def add_task(self, task):
        self.tasks.put(task, timeout=60)