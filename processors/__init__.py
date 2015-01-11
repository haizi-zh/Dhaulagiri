# coding=utf-8
from Queue import Empty
import logging
import traceback
import signal

__author__ = 'zephyre'

import gevent


class BaseProcessor(object):
    name = 'base-processor'

    def __init__(self, *args, **kwargs):
        from gevent.queue import Queue

        self.progress = 0
        self.total = 0
        self.maxsize = 1000
        self.op_done = False
        self.tasks = Queue(self.maxsize)
        self.jobs = []

        from time import time
        from hashlib import md5
        # 每个Processor的唯一标识
        self.processor_name = '%s:%s' % (self.name, md5(str(time())).hexdigest()[:6])

        self._logger = None

        arg_parser = kwargs['arg_parser']
        arg_parser.add_argument('--concur', default=10, type=int)
        self.arg_parser = arg_parser
        ret, leftover = arg_parser.parse_known_args()
        self.concur = ret.concur

    def log(self, msg, level=logging.INFO):
        if not self._logger:
            self._logger = logging.getLogger(self.processor_name)

        self._logger.log(level, msg)

    def join(self):
        self.op_done = True
        gevent.joinall(self.jobs[1:])
        gevent.kill(self.jobs[0])

    def run(self):
        import sys

        self.log('Processor started: %s' % ' '.join(sys.argv))

    def start_workers(self):
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