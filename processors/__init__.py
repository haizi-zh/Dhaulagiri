# coding=utf-8
from Queue import Empty
import signal

from core import LoggerMixin


__author__ = 'zephyre'

import gevent


class Worker(object):
    __index = 0

    def _run(self):
        import gevent.threading as threading

        self.logger.debug('Worker started: %s' % self.worker_name)
        g = threading.getcurrent()
        setattr(g, 'worker_name', self.worker_name)

        task_tracker = self.processor.engine.task_tracker

        while True:
            self.idle = True
            self.logger.debug('Retrieving next task...')
            try:
                task = self._task_queue.get(block=True)
            except Empty:
                continue
            finally:
                self.idle = False

            if task_tracker:
                # Task tracking机制已启用
                if task_tracker.track(task):
                    self.logger.info('Task %s bypassed' % getattr(task, 'task_key'))
                    continue

            self.logger.debug('Task started')
            try:
                ret = task()
                # 满足一致性。如果ret不是iterable，则将其转换为列表
                if not hasattr(ret, '__iter__'):
                    ret = [ret]

                for r in ret:
                    if hasattr(r, '__call__'):
                        # 返回值是一个回调函数
                        self.processor.add_task(r)

                if task_tracker:
                    task_tracker.update(task)
            except Exception as e:
                if e.message:
                    self.logger.error('Error occured: %s' % e.message, exc_info=True)
                else:
                    self.logger.error('Error occured: unknown', exc_info=True)

            self.logger.debug('Task completed')
            self.processor.incr_progress()

            gevent.sleep(0)

    def __init__(self, processor, queue, idx):
        self._task_queue = queue
        self.idle = False
        self.processor = processor
        self.logger = processor.logger
        # worker的编号和名字
        self.idx = idx
        self.worker_name = 'worker:%d' % self.idx

        self.gevent = gevent.spawn(self._run)

    @classmethod
    def from_processor(cls, processor, queue):
        Worker.__index += 1
        return Worker(processor, queue, Worker.__index)


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
        from threading import Lock

        self.processor_name = '%s:%s' % (self.name, md5(str(time())).hexdigest()[:6])

        LoggerMixin.__init__(self)

        self.engine = engine

        self.__redis = None
        self.redis_lock = Lock()

        self.progress = 0
        self.total = 0
        # 超过这一限制时，add_task就暂停向其中添加任务
        self.maxsize = 1000
        self.tasks = Queue()
        self.workers = []

        # 默认的polling间隔为1秒
        self.polling_interval = 1

        self.arg_parser = self.engine.arg_parser
        # 并发数量
        self.arg_parser.add_argument('--concur', default=20, type=int)
        ret, leftover = self.arg_parser.parse_known_args()
        self.args = ret
        self.concur = ret.concur

        self.checkpoint_ts = None
        self.checkpoint_prog = None
        self.init_ts = time()

        # 心跳任务
        self.heart_beat = None

    def incr_progress(self):
        self.progress += 1

    def _start_workers(self):
        def timer():
            """
            每30秒启动一次，输出当前进度
            """
            import time

            while True:
                msg = 'Progress: %d / %d.' % (self.progress, self.total)
                cts = time.time()

                if self.checkpoint_prog is not None and self.checkpoint_ts is not None:
                    rate = (self.progress - self.checkpoint_prog) / (cts - self.checkpoint_ts) * 60
                    msg = '%s %s' % (msg, 'Processing rate: %d items/min' % int(rate))

                self.checkpoint_ts = cts
                self.checkpoint_prog = self.progress

                self.log(msg)
                gevent.sleep(30)

        self.heart_beat = gevent.spawn(timer)

        gevent.signal(signal.SIGKILL, gevent.kill)
        gevent.signal(signal.SIGQUIT, gevent.kill)

        for i in xrange(self.concur):
            worker = Worker.from_processor(self, self.tasks)
            self.workers.append(worker)

    def add_task(self, task, *args, **kwargs):
        while True:
            # 如果self.tasks中的项目过多，则暂停添加
            if self.tasks.qsize() > self.maxsize:
                gevent.sleep(self.polling_interval)
            else:
                break

        func = lambda: task(*args, **kwargs)
        task_key = getattr(task, 'task_key', None)
        if task_key:
            setattr(func, 'task_key', task_key)
        self.tasks.put(func, timeout=120)
        gevent.sleep(0)

    def _wait_for_workers(self):
        """
        等待所有的worker是否完成。判据：所有的worker都处于idle状态，并且tasks队列已空
        :return:
        """
        while True:
            if not self.tasks.empty():
                gevent.sleep(self.polling_interval)
                continue

            completed = True
            for w in self.workers:
                if not w.idle:
                    gevent.sleep(self.polling_interval)
                    completed = False
                    break

            if completed:
                break

        gevent.killall([w.gevent for w in self.workers])
        gevent.kill(self.heart_beat)

    def run(self):
        self._start_workers()
        self.populate_tasks()
        self._wait_for_workers()

        import time

        self.log('Processor ended. %d items processed in %d minutes' % (self.progress,
                                                                        int((time.time() - self.init_ts) / 60.0)))

    def populate_tasks(self):
        raise NotImplementedError
