# coding=utf-8
import re
from lxml import etree

from processors import BaseProcessor
from utils.database import get_mongodb


__author__ = 'zephyre'


class BaseFactory(object):
    def generator(self):
        raise NotImplementedError


class QunarFactory(BaseFactory):
    def generator(self):
        col = get_mongodb('raw_data', 'QunarPoi', 'mongo-raw')
        for entry in col.find({'distName': u'北京'}, {'lat': 1, 'lng': 1, 'distName': 1, 'name': 1}):
            yield {'name': entry['name'], 'locality': entry['distName'],
                   'coordinate': {'lat': entry['lat'], 'lng': entry['lng']}}


class FactoryBuilder(object):
    def __init__(self):
        pass

    @staticmethod
    def get_factory(factory_name):
        """
        通过factory_name，获得对应的generator
        """
        if factory_name == 'qunar':
            return QunarFactory()
        else:
            raise ValueError('Invalid factory name: %s' % factory_name)


class DianpingProcessor(BaseProcessor):
    """
    处理大众点评的数据

    通过指定一个populator来获得入口数据。然后，调用大众点评的网站接口，获得更多的信息
    """

    name = 'dianping'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)

    @staticmethod
    def find_city(city_name):
        """
        根据城市名称，在大众点评的城市列表中进行搜索
        """
        col = get_mongodb('raw_dianping', 'City', 'mongo-raw')
        ret = col.find_one({'city_name': city_name}, {'city_id': 1})
        if ret:
            return ret['city_id']
        else:
            return None

    def parse_shop_details(self, response):
        """
        解析店面的详情
        """
        pass

    def populate_tasks(self):
        populator = FactoryBuilder().get_factory('qunar')
        cursor = populator.generator()

        for val in cursor:
            def task(entry=val):
                city_id = self.find_city(entry['locality'])
                if not city_id:
                    return
                url = 'http://www.dianping.com/search/keyword/%d/0_%s/p1' % (city_id, entry['name'])
                response = self.request.get(url)

                tree_node = etree.fromstring(response.text, parser=etree.HTMLParser())
                for shop_href in tree_node.xpath('//div[contains(@class,"shop-list")]/ul/li'
                                                 '//a[@onclick and @title and @href]/@href'):
                    match = re.search(r'shop/(\d+)', shop_href)
                    if not match:
                        continue
                    shop_id = int(match.group(1))

                    def fetch_shop_details(val=shop_id):
                        shop_resposne = self.request.get('http://www.dianping.com/shop/%d' % val)
                        self.parse_shop_details(shop_resposne)

                    yield fetch_shop_details

            self.add_task(task)
