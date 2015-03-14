# coding=utf-8
import logging
import re

from gevent.lock import BoundedSemaphore

from middlewares import MiddlewareManager
from utils import load_yaml


__author__ = 'zephyre'

dhaulagiri_settings = load_yaml()


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
        parser.add_argument('--quiet', action='store_true')
        parser.add_argument('--log2file', action='store_true')
        parser.add_argument('--debug', action='store_true')
        parser.add_argument('--logpath', type=str)
        args, leftovers = parser.parse_known_args()

        if 'logging' not in dhaulagiri_settings:
            dhaulagiri_settings['logging'] = {}

        if args.log2file:
            dhaulagiri_settings['logging']['write_to_file'] = True
        if args.quiet:
            dhaulagiri_settings['logging']['write_to_stream'] = False
        if args.debug:
            dhaulagiri_settings['logging']['log_level'] = logging.DEBUG
        if args.logpath:
            dhaulagiri_settings['logging']['log_path'] = args.logpath

        import os
        from logging.handlers import TimedRotatingFileHandler
        from logging import StreamHandler, Formatter

        name = getattr(self, 'name', 'general_logger')

        # Set up a specific logger with our desired output level
        from hashlib import md5
        from random import randint
        import sys

        sig = md5('%d' % randint(0, sys.maxint)).hexdigest()[:8]
        logger = logging.getLogger('%s-%s' % (name, sig))

        handler_list = []
        if dhaulagiri_settings['logging']['write_to_stream']:
            handler_list.append(StreamHandler())
        if dhaulagiri_settings['logging']['write_to_file']:
            log_path = os.path.abspath(dhaulagiri_settings['logging']['log_path'])

            try:
                os.mkdir(log_path)
            except OSError:
                pass

            log_file = os.path.normpath(os.path.join(log_path, '%s.log' % name))
            handler = TimedRotatingFileHandler(log_file, when='D', interval=1, encoding='utf-8')
            handler_list.append(handler)

        log_level = dhaulagiri_settings['logging']['log_level']
        formatter = Formatter(fmt='%(asctime)s [%(name)s] [%(threadName)s] %(levelname)s: %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S%z')

        if not handler_list:
            handler_list.append(logging.NullHandler())
        for handler in handler_list:
            handler.setLevel(log_level)
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        logger.setLevel(log_level)

        return logger


class TaskTrackerFactory(object):
    @classmethod
    def get_instance(cls, engine, tracker_name, expire):
        return RedisTaskTracker(engine.redis_cli, expire)


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


class RedisTaskTracker(BaseTaskTracker):
    """
    使用Redis作为TaskTracker
    """

    def __init__(self, redis, expire):
        """
        初始化

        :param redis: RedisClient对象
        :param expire: Task的过期时间
        :return:
        """
        self.__redis = redis
        self.expire = expire

    def track(self, task):
        r = self.__redis
        task_key = getattr(task, 'task_key', None)
        if not task_key:
            return False
        else:
            return r.exists(task_key)

    def update(self, task):
        r = self.__redis
        task_key = getattr(task, 'task_key', None)
        if task_key:
            r.set(task_key, True, self.expire)


class RedisClient(object):
    @staticmethod
    def _init_redis():
        import redis
        from utils import load_yaml

        cfg = load_yaml()
        redis_conf = filter(lambda v: v['profile'] == 'task-track', cfg['redis'])[0]
        host = redis_conf['host']
        port = int(redis_conf['port'])

        return redis.StrictRedis(host=host, port=port, db=0)

    def _get_redis(self):
        return self._redis

    redis = property(_get_redis)

    def __init__(self):
        self._redis = self._init_redis()

    def get(self, key):
        return self._redis.get(key)

    def set(self, key, value, expire=None):
        self._redis.set(key, value)
        if expire:
            self._redis.expire(key, expire)

    def exists(self, key):
        return self._redis.exists(key)

    def get_cache(self, key, retrieve_func=None, expire=None, refresh=False):
        """
        获得缓存内容
        :param key:
        :param retrieve_func: 当key不存在的时候，通过这一函数来获得数据
        :param expire: 指定过期时间（秒）
        :param refresh: 强制刷新缓存
        :return:
        """
        if (not self._redis.exists(key) or refresh) and retrieve_func:
            value = retrieve_func()
            self._redis.set(key, value)
            if expire:
                self._redis.expire(key, expire)
            return value
        else:
            return self._redis.get(key)


class ProcessorEngine(LoggerMixin):
    name = 'processor_engine'

    # Singleton
    __lock = BoundedSemaphore(1)

    __instance = None

    def _get_redis_cli(self):
        return self._redis_client

    redis_cli = property(_get_redis_cli)

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

    def parse_tracking(self, args):
        if not args.track:
            return

        # 默认有效期为3天
        expire = 3600 * 24 * 3

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

        engine = self
        return TaskTrackerFactory.get_instance(engine, args.track, expire)

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

        self._redis_client = RedisClient()

        # 获得TaskTracker
        self.task_tracker = self.parse_tracking(args)

        self.request = RequestHelper.from_engine(self)
        self.middleware_manager = MiddlewareManager.from_engine(self)

        self.processor_store = self.reg_processors()
        self.processors = {}

        self.log('Engine init completed')

    @staticmethod
    def _init_redis():
        import redis
        from utils import load_yaml

        cfg = load_yaml()
        redis_conf = filter(lambda v: v['profile'] == 'task-track', cfg['redis'])[0]
        host = redis_conf['host']
        port = int(redis_conf['port'])

        return redis.StrictRedis(host=host, port=port, db=0)

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


