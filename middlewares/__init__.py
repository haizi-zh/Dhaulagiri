# coding=utf-8
__author__ = 'zephyre'


class MiddlewareManager(object):
    """
    Maintain a list of middlewares
    """

    def __init__(self, engine=None):
        self.engine = engine
        self.mw_dict = {}
        self.load_mw()

    def load_mw(self):
        """
        Load middlewares from the configuration
        """
        from importlib import import_module

        for mw_type in ['download']:
            mw_list = []
            for mw in self.engine.settings['middlewares'][mw_type] if 'middlewares' in self.engine.settings else []:
                try:
                    ret = mw['name'].split('.')
                    module_path = '.'.join(ret[:-1])
                    class_name = ret[-1]
                    mw_cls = getattr(import_module(module_path), class_name)
                    ret = mw_cls.from_manager(self)
                    if not ret:
                        continue
                    mw_list.append({'middleware': ret,
                                    'priority': mw['priority'] if 'priority' in mw else 0})
                except (ImportError, RuntimeError):
                    continue

                mw_list = sorted(mw_list, key=lambda v: v['priority'], reverse=True)
                self.mw_dict[mw_type] = mw_list

    @classmethod
    def from_engine(cls, engine):
        return MiddlewareManager(engine)


class DownloadMiddleware(object):
    """
    Base class of download middlewares
    """

    def __init__(self, manager):
        self._manager = manager

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def on_request(self, req, session=None, session_kwarags=None):
        return {'next': True, 'value': (req, session, session_kwarags)}

    def on_response(self, response):
        return {'next': True, 'value': response}

    def on_failure(self, request, s_args):
        return True


class ProxyMiddleware(DownloadMiddleware):
    """
    Proxify traffic
    """

    max_error = 5

    def load_proxies(self):
        response = self._manager.engine.request.get('http://api.taozilvxing.cn/core/misc/proxies?'
                                                    'verifier=all&latency=2&pageSize=500&recently=24')

        def func(entry):
            proxy = '%s://%s:%d' % (entry['scheme'], entry['host'], entry['port'])
            return proxy, {'failCnt': 0, 'reqCnt': 0}

        return dict(map(func, filter(lambda v: not v['user'], response.json()['result'])))

    def __init__(self, manager):
        DownloadMiddleware.__init__(self, manager)

        parser = manager.engine.arg_parser
        parser.add_argument('--proxy', action='store_true')
        args, leftover = parser.parse_known_args()
        if not args.proxy:
            raise RuntimeError

        self.proxies = self.load_proxies()
        self.dead_proxies = {}

    def __fetch(self):
        from random import randint

        while True:
            if not self.proxies:
                # No available proxies
                self._manager.engine.logger.warn('No available proxies.')
                return

            try:
                plist = self.proxies.keys()
                proxy = plist[randint(0, len(plist) - 1)]
                self.proxies[proxy]['reqCnt'] += 1
                return proxy
            except (ValueError, IndexError):
                pass

    def on_request(self, req, session=None, session_kwarags=None):
        if 'proxies' not in session_kwarags:
            proxy = self.__fetch()
            if proxy:
                session_kwarags['proxies'] = {'http': proxy}

        return {'next': True, 'value': (req, session, session_kwarags)}

    def drop_proxy(self, proxy_name):
        if proxy_name in self.proxies:
            self._manager.engine.logger.warn('Disable proxy: %s' % proxy_name)
            p = self.proxies.pop(proxy_name)
            self.dead_proxies[proxy_name] = p
            self._manager.engine.logger.info(
                'Available proxies: %d, disabled proxies: %d' % (len(self.proxies), len(self.dead_proxies)))

    def add_fail_cnt(self, proxy_name):
        if proxy_name not in self.proxies:
            return

        self.proxies[proxy_name]['failCnt'] += 1
        self._manager.engine.logger.debug(
            'Proxy: %s failCnt added to %d' % (proxy_name, self.proxies[proxy_name]['failCnt'] ))
        if self.proxies[proxy_name]['failCnt'] > self.max_error:
            self.drop_proxy(proxy_name)

    def on_failure(self, request, s_args):
        if 'proxies' in s_args:
            self.add_fail_cnt(s_args['proxies']['http'])

        return True

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

        try:
            validator = user_data['ProxyMiddleware']['validator'] if user_data else {}
            success = validator(response)
        except KeyError:
            success = self.default_validator(response)
        result['success'] = success

        tmp = response.connection.proxy_manager.keys()
        if tmp and tmp[0] in self.proxies:
            proxy_name = tmp[0]
            if success:
                self.proxies[proxy_name]['failCnt'] = 0
            else:
                self._manager.engine.logger.debug('Proxy: %s failed in validation' % proxy_name)
                self.add_fail_cnt(proxy_name)
                result['next'] = False

        return result