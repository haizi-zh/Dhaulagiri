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