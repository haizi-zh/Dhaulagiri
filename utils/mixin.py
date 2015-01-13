import json
import re

__author__ = 'zephyre'


def baidu_suggestion(aClass):
    class Wrapper(object):

        base_cls = aClass

        def __init__(self, *args, **kwargs):
            self.wrapped = aClass(*args, **kwargs)

            self.wrapped.get_baidu_sug = self.get_baidu_sug

        def __getattr__(self, attrname):
            return getattr(self.wrapped, attrname)

        @staticmethod
        def get_baidu_sug(name, location):
            import requests
            from utils import mercator2wgs, haversine

            url = u'http://lvyou.baidu.com/destination/ajax/sug?wd=%s&prod=lvyou_new&su_num=20' % name

            try:
                sug = json.loads(requests.get(url).json()['data']['sug'])
                result = []
                for s in sug['s']:
                    tmp = re.split(r'\$', s)
                    entry = {'sname': tmp[0].strip(),
                             'parents': tmp[6].strip(),
                             'sid': tmp[8].strip(),
                             'surl': tmp[22].strip(),
                             'parent_sid': tmp[26].strip(),
                             'type_code': int(tmp[24])}

                    mx = float(tmp[14])
                    my = float(tmp[16])
                    entry['lng'], entry['lat'] = mercator2wgs(mx, my)

                    result.append(entry)

                return result
            except (ValueError, KeyError):
                return None

    return Wrapper

