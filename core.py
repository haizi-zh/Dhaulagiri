# coding=utf-8
import logging
import re
from time import time

from gevent.lock import BoundedSemaphore

from middlewares import MiddlewareManager


__author__ = 'zephyre'


class LoggerMixin(object):
    def __init__(self):
        from gevent.lock import BoundedSemaphore

        self.__logger_sem = BoundedSemaphore(1)
        self.__logger = None

    def _get_logger(self):
        if not self.__logger:
            try:
                self.__logger_sem.acquire()
                if not self.__logger:
                    self.__logger = self.__init_logger()
            finally:
                self.__logger_sem.release()

        return self.__logger

    logger = property(_get_logger, doc="Get the logger of the engine")

    def log(self, msg, level=logging.INFO):
        self.logger.log(level, msg)

    def __init_logger(self):
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument('--verbose', action='store_true')
        parser.add_argument('--debug', action='store_true')
        parser.add_argument('--logpath', type=str)
        args, leftovers = parser.parse_known_args()

        import os
        import logging
        from logging.handlers import TimedRotatingFileHandler
        from logging import StreamHandler, Formatter

        name = getattr(self, 'name', 'general_logger')

        # Set up a specific logger with our desired output level
        from hashlib import md5
        from random import randint
        import sys

        sig = md5('%d' % randint(0, sys.maxint)).hexdigest()[:8]
        logger = logging.getLogger('%s-%s' % (name, sig))

        if args.verbose:
            handler = StreamHandler()
        else:
            if args.logpath:
                log_path = os.path.abspath(args.logpath)
            else:
                log_path = '/var/log/dhaulagiri'
                # log_path = os.path.abspath(os.path.join(os.path.split(__file__)[0], '../log'))
            try:
                os.mkdir(log_path)
            except OSError:
                pass

            log_file = os.path.normpath(os.path.join(log_path, '%s.log' % name))
            handler = TimedRotatingFileHandler(log_file, when='d', interval=1, encoding='utf-8')

        log_level = logging.DEBUG if args.debug else logging.INFO
        handler.setLevel(log_level)

        formatter = Formatter(fmt='%(asctime)s [%(name)s] [%(threadName)s] %(levelname)s: %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S%z')
        handler.setFormatter(formatter)

        logger.addHandler(handler)
        logger.setLevel(log_level)

        return logger


class TaskTrackerFactory(object):
    @classmethod
    def get_instance(cls, tracker_name, expire):
        return DefaultTaskTracker(expire)


class BaseTaskTracker(object):
    """
    记录某个任务是否被执行
    """

    def track(self, task):
        """
        如果task可以bypass，则返回True

        :param task:
        :return:
        """
        raise NotImplementedError

    def update(self, task):
        """
        更新task的tracking信息
        :param task:
        :return:
        """
        raise NotImplementedError


class DefaultTaskTracker(BaseTaskTracker):
    def __init__(self, expire):
        BaseTaskTracker.__init__(self)

        self.__redis = None
        self.expire = expire

        from threading import Lock

        self.__redis_lock = Lock()

    def track(self, task):
        r = self.redis

        if not r:
            return False

        task_key = getattr(task, 'task_key', None)
        if not task_key:
            return False

        ret = r.get(task_key)
        if not ret:
            return False
        else:
            # 判断是否过期
            return time() < float(ret) + self.expire

    def update(self, task):
        r = self.redis
        if not r:
            return False

        task_key = getattr(task, 'task_key', None)
        if not task_key:
            return

        r.set(task_key, time())

    def __get_redis(self):
        if not self.__redis:
            try:
                self.__redis_lock.acquire()
                if not self.__redis:
                    import redis

                    from utils import load_yaml

                    cfg = load_yaml()
                    redis_conf = filter(lambda v: v['profile'] == 'task-track', cfg['redis'])[0]
                    host = redis_conf['host']
                    port = int(redis_conf['port'])

                    self.__redis = redis.StrictRedis(host=host, port=port, db=0)
            except (KeyError, IOError, IndexError):
                self.__redis = None
            finally:
                self.__redis_lock.release()

        return self.__redis

    redis = property(__get_redis)


class ProcessorEngine(LoggerMixin):
    name = 'processor_engine'

    # Singleton
    __lock = BoundedSemaphore(1)

    __instance = None

    @classmethod
    def get_instance(cls):
        if not cls.__instance:
            try:
                cls.__lock.acquire()
                if not cls.__instance:
                    cls.__instance = ProcessorEngine()
            finally:
                cls.__lock.release()

        return cls.__instance

    @staticmethod
    def reg_processors(proc_dir=None):
        """
        将processors路径下的processor类进行注册
        """
        import os
        import imp
        import types

        if not proc_dir:
            root_dir = os.path.normpath(os.path.split(__file__)[0])
            proc_dir = os.path.normpath(os.path.join(root_dir, 'processors'))

        processor_dict = {}

        for cur, d_list, f_list in os.walk(proc_dir):
            for f in f_list:
                f = os.path.normpath(os.path.join(cur, f))
                tmp, ext = os.path.splitext(f)
                if ext != '.py':
                    continue
                p, fname = os.path.split(tmp)

                try:
                    ret = imp.find_module(fname, [p]) if p else imp.find_module(fname)
                    mod = imp.load_module(fname, *ret)

                    for attr_name in dir(mod):
                        try:
                            target_cls = getattr(mod, attr_name)
                            name = getattr(target_cls, 'name')
                            func = getattr(target_cls, 'run')
                            if isinstance(name, str) and isinstance(func, types.MethodType):
                                processor_dict[name] = target_cls
                            else:
                                continue
                        except (TypeError, AttributeError):
                            pass
                except ImportError:
                    print 'Import error: %s' % fname
                    raise

        return processor_dict

    @staticmethod
    def parse_tracking(args):
        # 默认有效期为1天
        expire = 3600 * 24

        if args.track_exp:
            match = re.search(r'([\d\.]+)(\w)', args.track_exp)
            val = float(match.group(1))
            unit = match.group(2)
            if unit == 'd':
                expire = val * 3600 * 24
            elif unit == 'h':
                expire = val * 3600
            elif unit == 'm':
                expire = val * 60
            elif unit == 's':
                expire = val

        return TaskTrackerFactory.get_instance(args.track, expire) if args.track else None

    def __init__(self):
        import argparse
        from utils import load_yaml

        self.settings = load_yaml()

        LoggerMixin.__init__(self)

        # Base argument parser
        parser = argparse.ArgumentParser()
        parser.add_argument('--track', action='store_true')
        # task tracking的有效期。支持以下格式1d, 1h, 1m, 1s
        parser.add_argument('--track-exp', default=None, type=str)
        args, leftover = parser.parse_known_args()
        self.arg_parser = parser

        # 获得TaskTracker
        self.task_tracker = self.parse_tracking(args)

        self.request = RequestHelper.from_engine(self)
        self.middleware_manager = MiddlewareManager.from_engine(self)

        self.processor_store = self.reg_processors()
        self.processors = {}

        self.log('Engine init completed')

    def add_processor(self, name):
        if name not in self.processor_store:
            self.logger.critical('Cannot find processor: %s' % name)
            return

        processor = self.processor_store[name].from_engine(self)

        if name not in self.processors:
            self.processors[name] = []
        self.processors[name].append(processor)
        self.log('Added processor %s' % name)

    def start(self):
        self.log('Starting engine...')

        for processor_list in self.processors.values():
            for processor in processor_list:
                self.log('Starting processor %s' % processor.name)
                processor.run()
                self.log('Cleaning up processor %s' % processor.name)

        self.log('Cleaning up engine...')


class RequestHelper(object):
    def __init__(self, engine=None):
        self._engine = engine

    @classmethod
    def from_engine(cls, engine):
        return RequestHelper(engine)

    @staticmethod
    def get_default_header():
        return {'User-Agent':
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/40.0.2214.115 Safari/537.36',
                'Accept-Encoding': 'gzip, deflate, sdch',
                'Accept-Language': 'en,zh-CN;q=0.8,zh;q=0.6,zh-TW;q=0.4,en-US;q=0.2'}

    def request(self, method, url, params=None, data=None, headers=None, cookies=None, files=None, auth=None,
                hooks=None, json=None, timeout=None, allow_redirects=True, proxies=None, retry=5, user_data=None):
        """Constructs and sends a :class:`Request <Request>`.
        Returns :class:`Response <Response>` object.

        :param method: method for the new :class:`Request` object.
        :param url: URL for the new :class:`Request` object.
        :param params: (optional) Dictionary or bytes to be sent in the query string for the :class:`Request`.
        :param data: (optional) Dictionary, bytes, or file-like object to send in the body of the :class:`Request`.
        :param json: (optional) json data to send in the body of the :class:`Request`.
        :param headers: (optional) Dictionary of HTTP Headers to send with the :class:`Request`.
        :param cookies: (optional) Dict or CookieJar object to send with the :class:`Request`.
        :param files: (optional) Dictionary of ``'name': file-like-objects`` (or ``{'name': ('filename', fileobj)}``) for multipart encoding upload.
        :param auth: (optional) Auth tuple to enable Basic/Digest/Custom HTTP Auth.
        :param timeout: (optional) How long to wait for the server to send data
            before giving up, as a float, or a (`connect timeout, read timeout
            <user/advanced.html#timeouts>`_) tuple.
        :type timeout: float or tuple
        :param allow_redirects: (optional) Boolean. Set to True if POST/PUT/DELETE redirect following is allowed.
        :type allow_redirects: bool
        :param proxies: (optional) Dictionary mapping protocol to the URL of the proxy.

        Usage::

        """

        from requests import Request, Session

        mw_manager = getattr(self._engine, 'middleware_manager', {})
        if mw_manager and 'download' in mw_manager.mw_dict:
            mw_list = self._engine.middleware_manager.mw_dict['download']
        else:
            mw_list = []

        for idx in xrange(retry):
            session = Session()
            session_args = {'timeout': timeout, 'allow_redirects': allow_redirects, 'proxies': proxies}

            try:
                if not headers:
                    headers = self.get_default_header()
                prepped = Request(method=method, url=url, headers=headers, files=files, data=data, params=params,
                                  auth=auth, cookies=cookies, hooks=hooks).prepare()
                for entry in mw_list:
                    mw = entry['middleware']
                    ret = mw.on_request(prepped, session, session_args, user_data=user_data)
                    prepped, session, session_args = ret['value']
                    pass_next = ret['next']
                    if not pass_next:
                        break

                try:
                    response = session.send(prepped, **session_args)
                except IOError as e:
                    for entry in mw_list:
                        mw = entry['middleware']
                        pass_next = mw.on_failure(prepped, session_args)
                        if not pass_next:
                            break
                    raise e

                success = True
                for entry in mw_list:
                    mw = entry['middleware']
                    ret = mw.on_response(response, user_data=user_data)
                    response = ret['value']
                    pass_next = ret['next']
                    success = ret['success']
                    if not pass_next:
                        break

                if success:
                    return response

            except IOError as e:
                # 最多尝试次数：retry
                if idx < retry - 1:
                    continue
                else:
                    raise e

        raise IOError

    def get(self, url, retry=10, user_data=None, **kwargs):
        return self.request(method='GET', url=url, retry=retry, user_data=user_data, **kwargs)


