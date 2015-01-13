# coding=utf-8
from Queue import Empty
import logging
import signal


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

        self.arg_parser = kwargs['arg_parser']
        self.arg_parser.add_argument('--concur', default=10, type=int)
        self.arg_parser.add_argument('--verbose', action='store_true', default=False)
        self.arg_parser.add_argument('--debug', action='store_true', default=False)
        self.arg_parser.add_argument('--logpath', type=str)
        ret, leftover = self.arg_parser.parse_known_args()
        self.args = ret
        self.concur = ret.concur

        # Checkpoints for progress
        self.checkpoint_ts = None
        self.checkpoint_prog = None
        self.init_ts = time()

    def _get_logger(self):
        if self._logger:
            return self._logger

        import os
        import logging
        from logging.handlers import TimedRotatingFileHandler
        from logging import StreamHandler, Formatter

        # Set up a specific logger with our desired output level
        logger = logging.getLogger(self.processor_name)

        if self.args.verbose:
            handler = StreamHandler()
        else:
            if self.args.logpath:
                log_path = os.path.abspath(self.args.logpath)
            else:
                log_path = os.path.abspath(os.path.join(os.path.split(__file__)[0], '../log'))
            log_file = os.path.normpath(os.path.join(log_path, '%s.log' % self.name))
            handler = TimedRotatingFileHandler(log_file, when='d', encoding='utf-8')

        log_level = logging.DEBUG if self.args.debug else logging.INFO
        handler.setLevel(log_level)

        formatter = Formatter(fmt='%(asctime)s [%(name)s] %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S%z')
        handler.setFormatter(formatter)

        logger.addHandler(handler)
        logger.setLevel(log_level)
        self._logger = logger

        return logger

    logger = property(_get_logger)

    def log(self, msg, level=logging.INFO):
        self.logger.log(level, msg)

    def _join(self):
        self.op_done = True
        gevent.joinall(self.jobs[1:])
        gevent.kill(self.jobs[0])

    def _start_workers(self):
        def worker():
            while not self.tasks.empty() or not self.op_done:
                try:
                    task = self.tasks.get(timeout=1)
                except Empty:
                    continue

                try:
                    task()
                except Exception as e:
                    self.logger.error('Error occured!', exc_info=True)

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

        self.tasks.put(wrapper, timeout=30)
        gevent.sleep(0)

    def run(self):
        import sys
        import time

        self.log('Processor started: %s' % ' '.join(sys.argv))

        self._start_workers()

        self.populate_tasks()

        self._join()

        self.log('Processor ended. %d items processed in %d minutes' % (self.progress,
                                                                        int((time.time() - self.init_ts) / 60.0)))

    def populate_tasks(self):
        raise NotImplementedError
