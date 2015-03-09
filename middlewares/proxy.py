# coding=utf-8
from middlewares import DownloadMiddleware

__author__ = 'zephyre'


class ProxyMiddleware(DownloadMiddleware):
    """
    Proxify traffic
    """

    max_error = 5

    def load_proxies(self):
        """
        通过API接口，更新可用代理列表
        """

        response = self._manager.engine.request.get('http://api2.taozilvxing.cn/core/misc/proxies?'
                                                    'verifier=all&latency=2&pageSize=500&recently=24', proxies={})

        def func(entry):
            proxy = '%s://%s:%d' % (entry['scheme'], entry['host'], entry['port'])
            return proxy, {'failCnt': 0, 'reqCnt': 0}

        # 和现有的代理列表融合
        new_proxies = dict(map(func, filter(lambda v: not v['user'], response.json()['result'])))

        new_cnt = 0
        try:
            self.rw_lock.writer_acquire()
            for proxy_name, proxy_data in new_proxies.items():
                if proxy_name in self.dead_proxies:
                    continue

                if proxy_name not in self.proxies:
                    new_cnt += 1
                    self.proxies[proxy_name] = proxy_data

            self._manager.engine.logger.info('%d proxies added to the pool. Total proxies: %d, dead proxies: %d' %
                                             (new_cnt, len(self.proxies), len(self.dead_proxies)))
        finally:
            self.rw_lock.writer_release()

    def __init__(self, manager):
        from utils.locking import RWLock

        DownloadMiddleware.__init__(self, manager)

        parser = manager.engine.arg_parser
        parser.add_argument('--proxy', action='store_true')
        args, leftover = parser.parse_known_args()
        if not args.proxy:
            raise RuntimeError

        self.rw_lock = RWLock()

        self.proxies = {}
        # 被禁用的代理服务器列表
        self.dead_proxies = {}

        from threading import Timer

        # 每10分钟刷新一次代理列表
        refresh_interval = 600

        def task():
            manager.engine.logger.debug('Loading proxies...')
            self.load_proxies()
            t = Timer(refresh_interval, task, ())
            t.daemon = True
            t.start()

        task()

    def __fetch(self):
        from random import randint

        try:
            self.rw_lock.reader_acquire()

            if not self.proxies:
                # No available proxies
                self._manager.engine.logger.warn('No available proxies.')
                return

            plist = self.proxies.keys()
            proxy = plist[randint(0, len(plist) - 1)]
            self.proxies[proxy]['reqCnt'] += 1

            self._manager.engine.logger.debug('Proxy fetched: %s' % proxy)
            return proxy
        finally:
            self.rw_lock.reader_release()

    def on_request(self, req, session=None, session_kwarags=None, user_data=None):
        if 'proxies' not in session_kwarags or session_kwarags['proxies'] is None:
            proxy = self.__fetch()
            if proxy:
                session_kwarags['proxies'] = {'http': proxy}

        return {'next': True, 'value': (req, session, session_kwarags)}

    def drop_proxy(self, proxy_name):
        """
        禁用某个代理服务器
        """
        try:
            self.rw_lock.writer_acquire()
            if proxy_name in self.proxies:
                self._manager.engine.logger.warn('Disable proxy: %s' % proxy_name)
                p = self.proxies.pop(proxy_name)
                self.dead_proxies[proxy_name] = p
                self._manager.engine.logger.info(
                    'Available proxies: %d, disabled proxies: %d' % (len(self.proxies), len(self.dead_proxies)))
        finally:
            self.rw_lock.writer_release()

    def add_fail_cnt(self, proxy_name):
        drop_flag = False
        try:
            self.rw_lock.writer_acquire()
            if proxy_name not in self.proxies:
                return

            self.proxies[proxy_name]['failCnt'] += 1
            self._manager.engine.logger.debug(
                'Proxy: %s failCnt added to %d' % (proxy_name, self.proxies[proxy_name]['failCnt'] ))
            if self.proxies[proxy_name]['failCnt'] > self.max_error:
                drop_flag = True
        finally:
            self.rw_lock.writer_release()

        if drop_flag:
            self.drop_proxy(proxy_name)

    def reset_fail_cnt(self, proxy_name):
        try:
            self.rw_lock.writer_acquire()
            if proxy_name not in self.proxies:
                return
            self.proxies[proxy_name]['failCnt'] = 0
        finally:
            self.rw_lock.writer_release()

    def on_failure(self, request, s_args):
        if 'proxies' in s_args:
            self.add_fail_cnt(s_args['proxies']['http'])

        return False

    @staticmethod
    def default_validator(response):
        """
        默认的response验证器，通过判断HTTP status code来确定代理是否有效

        :param response:
        :return:
        """
        return response.status_code in [200, 301, 302, 304]

    def on_response(self, response, user_data=None):
        result = {'next': True, 'value': response, 'success': True}

        validator = self.default_validator
        if user_data:
            try:
                validator = user_data['ProxyMiddleware']['validator']
            except KeyError:
                pass

        success = validator(response)
        result['success'] = success

        tmp = response.connection.proxy_manager.keys()
        if tmp and tmp[0] in self.proxies:
            proxy_name = tmp[0]
            if success:
                self.reset_fail_cnt(proxy_name)
            else:
                self._manager.engine.logger.debug('Proxy: %s failed in validation' % proxy_name)
                self.add_fail_cnt(proxy_name)
                result['next'] = False

        return result
