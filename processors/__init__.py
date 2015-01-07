__author__ = 'zephyre'

import gevent


class BaseProcessor(object):
    def __init__(self):
        from gevent.queue import Queue

        self.progress = 0
        self.total = 0
        self.concur = BaseProcessor.args_builder().concur
        self.tasks = Queue()

    @staticmethod
    def args_builder():
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument('--concur', default=10, type=int)
        args, leftovers = parser.parse_known_args()
        return args

    def run(self):
        def worker():
            while not self.tasks.empty():
                task = self.tasks.get()
                task()
                gevent.sleep(0)

        def timer():
            while not self.tasks.empty():
                print 'Progress: %d / %d' % (self.progress, self.total)
                gevent.sleep(60)

        jobs = [gevent.spawn(timer)]
        for i in xrange(self.concur):
            jobs.append(gevent.spawn(worker))
        gevent.joinall(jobs)

    def add_task(self, task):
        self.tasks.put_nowait(task)