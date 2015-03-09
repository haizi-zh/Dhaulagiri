# coding=utf-8
import logging
import re
from lxml import etree

from processors import BaseProcessor
from utils.database import get_mongodb


__author__ = 'zephyre'


class DianpingHelper(object):
    def __init__(self):
        # 缓存的城市信息
        from gevent.lock import BoundedSemaphore

        self._city_cache = {}
        self._city_cache_lock = BoundedSemaphore(1)

    def get_city(self, city_name, coords):
        """
        通过城市名称和坐标，获得城市详情
        """
        if city_name not in self._city_cache:
            try:
                self._city_cache_lock.acquire()
                if city_name not in self._city_cache:
                    col = get_mongodb('geo', 'Locality', 'mongo')
                    lat = coords['lat']
                    lng = coords['lng']
                    geo_json = {'type': 'Point', 'coordinates': [coords['lng'], coords['lat']]}
                    max_distance = 200000
                    city_list = list(col.find(
                        {'alias': city_name,
                         'location': {'$near': {'$geometry': geo_json, '$maxDistance': max_distance}}}))
                    if city_list:
                        city = city_list[0]
                        self._city_cache[city_name] = city
                    else:
                        self._city_cache[city_name] = None
                        raise ValueError('Failed to find city: %s, lat=%f, lng=%f' % (city_name, lat, lng))
            finally:
                self._city_cache_lock.release()


class BaseFactory(object):
    def generator(self):
        raise NotImplementedError


class QunarFactory(BaseFactory):
    def generator(self):
        col = get_mongodb('raw_data', 'QunarPoi', 'mongo-raw')
        for entry in col.find({'distName': u'北京'}, {'lat': 1, 'lng': 1, 'distName': 1, 'name': 1}).limit(10):
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


class DianpingMatcher(BaseProcessor):
    """
    和大众点评的数据进行关联
    """

    name = 'dianping-matcher'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)

        self.build_args()

        self.city_map = {}
        self.build_city_map()

    def build_city_map(self):
        """
        建立从自有数据库的city到大众点评的city的映射
        """
        city_map = {}

        col_dp = get_mongodb('raw_dianping', 'City', 'mongo-raw')
        col_loc = get_mongodb('geo', 'Locality', 'mongo')
        for city_item in col_dp.find({}):
            city_name = city_item['city_name']
            candidates = list(col_loc.find({'alias': city_name}, {'_id': 1}))
            if len(candidates) > 1:
                self.log('Duplicate cities found for %s' % city_name, logging.WARN)
            elif not candidates:
                self.log('No city found for %s' % city_name, logging.WARN)
            else:
                city_id = candidates[0]['_id']
                city_map[city_id] = city_item

        self.city_map = city_map

    def build_args(self):
        """
        处理命令行参数
        """
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument('--limit', type=int)
        parser.add_argument('--skip', type=int, default=0)
        self.args, leftover = parser.parse_known_args()

    def build_cursor(self):
        col = get_mongodb('poi', 'Restaurant', 'mongo')
        cursor = col.find({'source.dianping.id': None, 'locality._id':{'$in':self.city_map.keys()}},
                          {'locality': 1, 'zhName': 1, 'alias': 1, 'location': 1}).skip(self.args.skip)
        if self.args.limit:
            cursor.limit(self.args.limit)
        return cursor

    def parse_search_list(self, response):
        pass

    def dianping_match(self, entry):
        """
        进行match操作
        """
        city_id = self.city_map[entry['locality']['_id']]
        shop_name = entry['zhName']
        location = entry['location']['coordinates']
        coords = {'lng': location[0], 'lat': location['1']}

        url = 'http://www.dianping.com/search/keyword/%d/0_%s'%(city_id, shop_name)
        response = self.request.get(url)



        pass

    def populate_tasks(self):
        for val in self.build_cursor():
            def task(entry=val):
                self.dianping_match(entry)

            self.add_task(task)


class DianpingFetcher(BaseProcessor):
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
        col = get_mongodb('raw_faq', 'CtripAnswer', 'mongo-raw')
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
                        shop_url = 'http://www.dianping.com/shop/%d' % val
                        self.log('Processing %s' % shop_url, logging.DEBUG)
                        shop_resposne = self.request.get(shop_url)
                        self.parse_shop_details(shop_resposne)

                    yield fetch_shop_details

            self.add_task(task)


class DianpingProcessor(BaseProcessor):
    name = 'dianping-shop'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)
        self.build_args()

        # 缓存的城市信息
        from gevent.lock import BoundedSemaphore

        self._city_cache = {}
        self._city_cache_lock = BoundedSemaphore(1)

    def build_args(self):
        """
        处理命令行参数
        """
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument('--limit', type=int)
        parser.add_argument('--skip', type=int, default=0)
        self.args, leftover = parser.parse_known_args()

    def build_cursor(self):
        col = get_mongodb('raw_dianping', 'Dining', 'mongo-raw')
        cursor = col.find({}).skip(self.args.skip)
        if self.args.limit:
            cursor.limit(self.args.limit)
        return cursor

    def populate_tasks(self):
        for val in self.build_cursor():
            def task(entry=val):
                self.process_details(entry)

            self.add_task(task)

    def get_city(self, city_name, coords):
        """
        通过城市名称和坐标，获得城市详情
        """
        if city_name not in self._city_cache:
            try:
                self._city_cache_lock.acquire()
                if city_name not in self._city_cache:
                    col = get_mongodb('geo', 'Locality', 'mongo')
                    lat = coords['lat']
                    lng = coords['lng']
                    geo_json = {'type': 'Point', 'coordinates': [coords['lng'], coords['lat']]}
                    max_distance = 200000
                    city_list = list(col.find(
                        {'alias': city_name,
                         'location': {'$near': {'$geometry': geo_json, '$maxDistance': max_distance}}}))
                    if city_list:
                        city = city_list[0]
                        self._city_cache[city_name] = city
                    else:
                        self.log('Failed to find city: %s, lat=%f, lng=%f' % (city_name, lat, lng), logging.WARN)
                        self._city_cache[city_name] = None
            finally:
                self._city_cache_lock.release()

        return self._city_cache[city_name]

    @staticmethod
    def calc_rating(entry):
        """
        计算店铺的rating
        """
        if 'reivew_stat' not in entry:
            return

        review = entry['reivew_stat']
        tmp = 0
        for idx in xrange(1, 6):
            key = 'reviewCountStar%d' % idx
            tmp += idx * review[key]
        total_cnt = review['reviewCountAllStar']
        if total_cnt == 0:
            return
        rating = float(tmp) / total_cnt

        return {'rating': rating, 'voteCnt': total_cnt}

    def process_details(self, entry):
        """
        处理店铺详情
        """
        city_info = self.get_city(entry['city_name'], {'lat': entry['lat'], 'lng': entry['lng']})
        if not city_info:
            return

        country = {}
        for key in ('_id', 'zhName', 'enName'):
            if key in city_info['country']:
                country[key] = city_info['country'][key]

        locality = {}
        for key in ('_id', 'zhName', 'enName', 'location'):
            if key in city_info:
                locality[key] = city_info[key]

        shop = {'source': {'dianping': {'id': entry['shop_id']}},
                'zhName': entry['title'], 'alias': [entry['title']],
                'address': entry['addr'],
                'location': {'type': 'Point', 'coordinates': [entry['lng'], entry['lat']]},
                'country': country, 'locality': locality, 'targets': [country['_id'], locality['_id']],
                'taoziEna': True, 'lxpEna': True}

        tags = []
        if 'tags' in entry and entry['tags']:
            for t in entry['tags']:
                tags.append(t)
        if 'cat_name' in entry and entry['cat_name']:
            cat_name = entry['cat_name']
            tags.append(cat_name)
            entry['style'] = cat_name
        tags = list(set(tags))
        if tags:
            shop['tags'] = tags

        fields_map = {'mean_price': 'price', 'tel': 'tel', 'open_time': 'openTime', 'cover_image': 'cover_image'}
        for key1, key2 in fields_map.items():
            if key1 in entry and entry[key1]:
                shop[key2] = entry[key1]

        score = self.calc_rating(entry)
        if score:
            shop['voteCnt'] = score['voteCnt']
            shop['rating'] = score['rating']

        self.update_shop(shop)

    @staticmethod
    def add_image(image_url):
        from hashlib import md5

        url_hash = md5(image_url).hexdigest()
        image = {'url_hash': url_hash, 'key': url_hash, 'url': image_url}
        col = get_mongodb('imagestore', 'ImageCandidates', 'mongo')
        col.update({'key': image['key']}, {'$set': image}, upsert=True)


    @staticmethod
    def update_shop(shop):
        """
        将店铺存储至数据库
        """
        if 'cover_image' in shop:
            cover = shop.pop('cover_image')
            DianpingProcessor.add_image(cover)

        add_to_set = {}
        for key in ('tags', 'alias'):
            if key in shop:
                value_list = shop.pop(key)
                add_to_set[key] = {'$each': value_list}
        ops = {'$set': shop}
        if add_to_set:
            ops['$addToSet'] = add_to_set

        col = get_mongodb('raw_dianping', 'DiningProc', 'mongo-raw')
        col.update({'source.dianping.id': shop['source']['dianping']['id']}, ops, upsert=True)