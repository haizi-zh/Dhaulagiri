# coding=utf-8
import logging

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
        logger = logging.getLogger(name)

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

        formatter = Formatter(fmt='%(asctime)s [%(name)s] %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S%z')
        handler.setFormatter(formatter)

        logger.addHandler(handler)
        logger.setLevel(log_level)

        return logger


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

    def __init__(self):
        import argparse
        from utils import load_yaml

        self.settings = load_yaml()

        LoggerMixin.__init__(self)

        # Base argument parser
        parser = argparse.ArgumentParser()
        parser.add_argument('cmd', type=str)

        self.arg_parser = parser

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


class RequestHelper(object):
    def __init__(self, engine=None):
        self._engine = engine

    @classmethod
    def from_engine(cls, engine):
        return RequestHelper(engine)

    def get(self, url, retry=10, user_data=None, **kwargs):
        from requests import Request, Session

        for idx in xrange(retry):
            try:
                req = Request(method='GET', url=url, headers=kwargs['headers'] if 'headers' in kwargs else None,
                              data=kwargs['data'] if 'data' in kwargs else None,
                              params=kwargs['params'] if 'params' in kwargs else None,
                              auth=kwargs['auth'] if 'auth' in kwargs else None,
                              cookies=kwargs['cookies'] if 'cookies' in kwargs else None)
                prepped = req.prepare()

                s = Session()
                s_args = {}

                mw_manager = getattr(self._engine, 'middleware_manager', {})

                if mw_manager and 'download' in mw_manager.mw_dict:
                    mw_list = self._engine.middleware_manager.mw_dict['download']
                else:
                    mw_list = []

                for entry in mw_list:
                    mw = entry['middleware']
                    ret = mw.on_request(prepped, s, s_args, user_data=user_data)
                    prepped, s, s_args = ret['value']
                    pass_next = ret['next']
                    if not pass_next:
                        break

                try:
                    response = s.send(prepped, **s_args)
                except IOError as e:
                    for entry in mw_list:
                        mw = entry['middleware']
                        pass_next = mw.on_failure(prepped, s_args)
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

        return None


