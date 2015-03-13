# coding=utf-8
import json
import logging
import re

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


def status_code_validator(response, allowed_codes):
    return response.status_code in allowed_codes


def response_size_validator(response, min_size=None, max_size=None):
    """
    根据response的size进行验证
    """
    sz = len(response.text)
    if min_size and sz < min_size:
        return False
    if max_size and sz > max_size:
        return False
    return True


class DianpingFetcher(BaseProcessor):
    """
    抓取大众点评的评论数据
    """

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)
        self.args = self.args_builder()

    def args_builder(self):
        parser = self.arg_parser
        parser.add_argument('--limit', default=None, type=int)
        parser.add_argument('--skip', default=0, type=int)
        parser.add_argument('--query', type=str)
        parser.add_argument('--batch-size', type=int)
        args, leftover = parser.parse_known_args()
        return args

    def build_cursor(self):
        col = get_mongodb('raw_dianping', 'Dining', 'mongo-raw')

        query = {}
        if self.args.query:
            exec 'from bson import ObjectId'
            query = eval(self.args.query)

        cursor = col.find(query, {'shop_id': 1})
        if self.args.limit:
            cursor.limit(self.args.limit)
        cursor.skip(self.args.skip)
        if self.args.batch_size:
            cursor.batch_size(self.args.batch_size)
        return cursor

    def populate_tasks(self):
        cursor = self.build_cursor()

        for val in cursor:
            def task(entry=val):
                self.process(entry)

            setattr(task, 'task_key', 'task:%s:%d' % (self.name, val['shop_id']))
            self.add_task(task)

    def process(self, entry):
        raise NotImplementedError


class DianpingCommentSpider(DianpingFetcher):
    """
    抓取大众点评POI的评论
    """

    name = 'dianping-comment'

    def process(self, entry):
        shop_id = entry['shop_id']
        self.parse_comment_page(shop_id)

    def parse_comment_page(self, shop_id, page_idx=1):
        template = 'http://www.dianping.com/shop/%d/review_all?pageno=%d'
        comment_url = template % (shop_id, page_idx)

        validators = [lambda v: status_code_validator(v, [200, 404]),
                      lambda v: response_size_validator(v, 4096)]

        response = self.request.get(comment_url, timeout=15, user_data={'ProxyMiddleware': {'validator': validators}})

        from lxml import etree

        col = get_mongodb('raw_dianping', 'DianpingComment', 'mongo-raw')
        root_node = etree.fromstring(response.text, parser=etree.HTMLParser())
        for comment_node in root_node.xpath('//div[@class="comment-list"]/ul/li[@data-id and @id]'):
            comment = self.parse_comment_details(shop_id, comment_node)
            col.update({'comment_id': comment['comment_id']}, {'$set': comment}, upsert=True)

        # 查看其它的页面
        if page_idx == 1:
            pages = map(int, root_node.xpath('//div[@class="Pages"]/a[@href and @data-pg]/@data-pg'))
            if not pages:
                return
            for page_idx in xrange(2, max(pages) + 1):
                self.parse_comment_page(shop_id, page_idx)

    def parse_comment_details(self, shop_id, comment_node):
        comment = {'shop_id': shop_id}

        comment_id = int(comment_node.xpath('./@data-id')[0])
        comment['comment_id'] = comment_id

        try:
            image_node = comment_node.xpath('./div[@class="pic"]/a[@user-id]/img[@title and @src]')[0]
            user_name = image_node.xpath('./@title')[0].strip()
            user_avatar = image_node.xpath('./@src')[0].strip()

            pattern = re.compile(r'(/pc/[0-9a-z]{32})\(\d+[cx]\d+\)/')
            if re.search(pattern, user_avatar):
                user_avatar = re.sub(pattern, '\\1(1024c1024)/', user_avatar)

            if user_name:
                comment['user_name'] = user_name
            if user_avatar:
                comment['user_avatar'] = user_avatar
        except IndexError:
            pass

        try:
            text_node = comment_node.xpath('./div[@class="content"]/div[@class="comment-txt"]'
                                           '/div[@class="J_brief-cont"]')[0]
            for br_node in text_node.xpath('.//br'):
                br_node.tail = '\n' + br_node.tail if br_node.tail else '\n'

            text_components = []
            for txt in text_node.itertext():
                if not txt or not txt.strip():
                    continue
                else:
                    text_components.append(txt.strip(' '))
            contents = ''.join(text_components).strip()
            if contents:
                comment['contents'] = contents
        except IndexError:
            pass

        try:
            rating_class = comment_node.xpath('./div[@class="content"]/div[@class="user-info"]'
                                              '/span[@title and @class]/@class')[0]
            match = re.search(r'irr-star(\d+)', rating_class)
            if match:
                comment['rating'] = float(match.group(1)) / 50
        except IndexError:
            pass

        try:
            time_text = comment_node.xpath('./div[@class="content"]/div[@class="misc-info"]'
                                           '/span[@class="time"]/text()')[0]
            ts = self.parse_comment_time(time_text)
            if ts:
                comment['ctime'] = ts
        except IndexError:
            pass

        return comment

    def parse_comment_time(self, time_text):
        from datetime import datetime, timedelta

        delta_ts = timedelta(seconds=8 * 3600)

        def guess_year(date):
            """
            传入的date可能缺失年份信息。比如：03/15。这样，只能取和当前时间最接近的年份。
            默认传入的date为东八区时间
            """
            cur_date = datetime.utcnow()
            init_year = cur_date.year + 1

            while True:
                try_date = datetime(init_year, date.month, date.day, date.hour, date.minute, date.second,
                                    date.microsecond) - delta_ts
                if try_date < cur_date:
                    return try_date
                else:
                    init_year -= 1

        pattern = r'(\d{2}-?){2,3} \d{2}:\d{2}'
        match = re.search(pattern, time_text)
        if match:
            time_text = match.group()
            if len(re.findall(r'(\d{2}-)', time_text)) == 1:
                format_str = '%m-%d %H:%M'
                ts = guess_year(datetime.strptime(time_text, format_str))
            else:
                format_str = '%y-%m-%d %H:%M'
                ts = datetime.strptime(time_text, format_str) - delta_ts

            return long((ts - datetime.utcfromtimestamp(0)).total_seconds()) * 1000

        pattern = r'(\d{2}-?){2,3}'
        match = re.search(pattern, time_text)
        if match:
            time_text = match.group()
            if len(re.findall(r'(\d{2}-)', time_text)) == 1:
                format_str = '%m-%d'
                ts = guess_year(datetime.strptime(time_text, format_str))
            else:
                format_str = '%y-%m-%d'
                ts = datetime.strptime(time_text, format_str) - delta_ts

            return long((ts - datetime.utcfromtimestamp(0)).total_seconds()) * 1000

        self.logger.warn('Invalid time string: %s' % time_text)
        return


class DianpingImageSpider(DianpingFetcher):
    """
    抓取点评POI的照片
    """

    name = 'dianping-image'

    def process(self, entry):
        shop_id = entry['shop_id']
        self.get_poi_image(shop_id)

    def get_poi_image(self, shop_id, page_idx=1):
        template = 'http://www.dianping.com/shop/%d/photos?pg=%d'
        album_url = template % (shop_id, page_idx)

        validators = [lambda v: status_code_validator(v, [200, 404]),
                      lambda v: response_size_validator(v, 4096)]

        response = self.request.get(album_url, timeout=15, user_data={'ProxyMiddleware': {'validator': validators}})

        from lxml import etree
        from hashlib import md5

        col = get_mongodb('raw_dianping', 'DianpingImage', 'mongo-raw')
        root_node = etree.fromstring(response.text, parser=etree.HTMLParser())
        for image_node in root_node.xpath('//div[@class="picture-list"]/ul/li[@class="J_list"]'):
            try:
                image_title = image_node.xpath(
                    './div[@class="picture-info"]/div[@class="name"]//a[@href and @title and @onclick]/@title')[0]
                if u'默认图片' in image_title:
                    continue
            except IndexError:
                continue

            try:
                image_src = image_node.xpath('./div[@class="img"]/a[@href and @onclick]/img[@src and @title]/@src')[0]
                pattern = re.compile(r'(/pc/[0-9a-z]{32})\(\d+[cx]\d+\)/')
                match = re.search(pattern, image_src)
                if not match:
                    continue

                image_src = re.sub(pattern, '\\1(1024c1024)/', image_src)
                key = md5(image_src).hexdigest()
                image_entry = {'url_hash': key, 'key': key, 'url': image_src}

                col.update({'key': key}, {'$set': image_entry}, upsert=True)
            except IndexError:
                continue


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

    def build_city_map(self, refresh_redis=False):
        """
        建立从自有数据库的city到大众点评的city的映射
        """
        redis = self.engine.redis
        city_map = {}

        col_dp = get_mongodb('raw_dianping', 'City', 'mongo-raw')
        col_loc = get_mongodb('geo', 'Locality', 'mongo')
        for city_item in col_dp.find({}):
            city_name = city_item['city_name']
            redis_key = 'dianping:norm_city_%s' % city_name
            norm_city_info = None

            if refresh_redis or not redis.exists(redis_key):
                candidates = list(col_loc.find({'alias': city_name}, {'_id': 1}))
                if len(candidates) > 1:
                    self.log('Duplicate cities found for %s' % city_name, logging.WARN)
                elif not candidates:
                    self.log('No city found for %s' % city_name, logging.WARN)
                else:
                    norm_city_info = candidates[0]

                redis.set(redis_key, norm_city_info)
            else:
                exec 'from bson import ObjectId'
                norm_city_info = eval(redis.get(redis_key))

            if norm_city_info:
                city_id = norm_city_info['_id']
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
        parser.add_argument('--query', type=str)
        self.args, leftover = parser.parse_known_args()

    def build_cursor(self):
        col = get_mongodb('poi', 'Restaurant', 'mongo')
        query = {'source.dianping.id': None, 'locality._id': {'$in': self.city_map.keys()}}
        if self.args.query:
            exec 'from bson import ObjectId'
            extra_query = eval(self.args.query)
        else:
            extra_query = {}
        if extra_query:
            query = {'$and': [query, extra_query]}

        cursor = col.find(query, {'locality': 1, 'zhName': 1, 'alias': 1, 'location': 1}).skip(self.args.skip)
        if self.args.limit:
            cursor.limit(self.args.limit)
        return cursor

    @staticmethod
    def get_shop_by_id(shop_id):
        col = get_mongodb('raw_dianping', 'Dining', 'mongo-raw')
        return col.find_one({'shop_id': shop_id}, {'lat': 1, 'lng': 1})

    def parse_shop_page(self, response, context):
        """
        解析单个搜索结果页
        :return 店铺详情的集合
        """
        from lxml import etree

        tree_node = etree.fromstring(response.text, parser=etree.HTMLParser())
        col = get_mongodb('raw_dianping', 'Dining', 'mongo-raw')
        for shop_href in tree_node.xpath('//div[contains(@class,"shop-list")]/ul/li'
                                         '//a[@href and @onclick and @title]/@href'):
            match = re.search(r'shop/(\d+)', shop_href)
            if not match:
                continue
            try:
                shop_id = int(match.group(1))
            except ValueError:
                continue
            context['shop_id'] = shop_id

            shop = col.find_one({'shop_id': shop_id})
            if shop:
                yield shop
            else:
                redis_key = 'dianping:shop_html_%d' % shop_id
                html_body = self.engine.redis.get(redis_key)
                if not html_body:
                    try:
                        html_body = self.request.get('http://www.dianping.com/shop/%d' % shop_id).text
                    except IOError:
                        continue

                shop_details = self.parse_shop_details(html_body, context)
                if shop_details:
                    self.engine.redis.set(redis_key, html_body)
                    yield shop_details

                    # TODO 暂时只返回一个店铺
                    return

    @staticmethod
    def get_dishes(html):
        """
        获得推荐菜品
        """
        from lxml import etree

        dishes = []
        sel = etree.fromstring(html, parser=etree.HTMLParser())
        for tmp in sel.xpath('//div[contains(@class,"shop-tab-recommend")]/p[@class="recommend-name"]'
                             '/a[@class="item" and @title]'):
            dish_name = tmp.xpath('./@title')[0].strip()
            # 去除首尾可能出现的句点
            dish_name = re.sub(r'\s*\.$', '', dish_name)
            dish_name = re.sub(r'^\.\s*', '', dish_name)
            recommend_cnt = 0

            tmp = tmp.xpath('./em[@class="count"]/text()')
            if tmp:
                match = re.search(r'\d+', tmp[0])
                if match:
                    recommend_cnt = int(match.group())
            dishes.append({'name': dish_name, 'recommend_cnt': recommend_cnt})

        return dishes

    def parse_shop_details(self, html_body, context):
        """
        解析店铺详情
        :return: 单个店铺详情
        """
        shop_id = context['shop_id']
        self.log('Fetching shop: %d' % shop_id, logging.INFO)

        from lxml import etree

        tree_node = etree.fromstring(html_body, parser=etree.HTMLParser())

        # 保证这是一个餐厅页面
        tmp = tree_node.xpath('//div[@class="breadcrumb"]/a[@href]/text()')
        if not tmp or u'餐厅' not in tmp[0]:
            return

        basic_info_node = tree_node.xpath('//div[@id="basic-info"]')[0]

        taste_rating = None
        env_rating = None
        service_rating = None
        mean_price = None

        def extract_rating(text):
            match2 = re.search(r'\d+\.\d+', text)
            if match2:
                return float(match2.group())
            else:
                return None

        for info_text in basic_info_node.xpath('.//div[@class="brief-info"]/span[@class="item"]/text()'):
            if info_text.startswith(u'口味'):
                taste_rating = extract_rating(info_text)
            elif info_text.startswith(u'环境'):
                env_rating = extract_rating(info_text)
            elif info_text.startswith(u'服务'):
                service_rating = extract_rating(info_text)
            elif info_text.startswith(u'人均'):
                match = re.search(r'\d+', info_text)
                if match:
                    mean_price = int(match.group())

        tel = None
        tmp = basic_info_node.xpath('.//p[contains(@class,"expand-info") and contains(@class,"tel")]'
                                    '/span[@itemprop="tel"]/text()')
        if tmp and tmp[0].strip():
            tel = tmp[0].strip()

        addr = None
        tmp = basic_info_node.xpath('.//div[contains(@class,"expand-info") and contains(@class,"address")]'
                                    '/span[@itemprop="street-address"]/text()')
        if tmp and tmp[0].strip():
            addr = tmp[0].strip()

        cover = None
        tmp = tree_node.xpath('//div[@id="aside"]//div[@class="photos"]/a[@href]/img[@itemprop="photo" and @src]/@src')
        if tmp:
            cover = tmp[0].strip()

        open_time = None
        tags = set([])
        desc = None
        for other_info_node in basic_info_node.xpath('.//div[contains(@class,"other")]/p[contains(@class,"info")]'):
            tmp = other_info_node.xpath('./span[@class="info-name"]/text()')
            if not tmp:
                continue
            info_name = tmp[0]
            if info_name.startswith(u'营业时间'):
                tmp = other_info_node.xpath('./span[@class="item"]/text()')
                if not tmp:
                    continue
                open_time = tmp[0].strip()
            elif info_name.startswith(u'分类标签'):
                tmp = other_info_node.xpath('./span[@class="item"]/a/text()')
                for tag in tmp:
                    tags.add(tag.strip())
            elif info_name.startswith(u'餐厅简介'):
                tmp = '\n'.join(filter(lambda v: v,
                                       (tmp.strip() for tmp in other_info_node.xpath('./text()')))).strip()
                if not tmp:
                    continue
                desc = tmp

        tmp = tree_node.xpath('//div[@id="shop-tabs"]/script/text()')
        if tmp:
            dishes = self.get_dishes(tmp[0])
        else:
            dishes = []

        lat = None
        lng = None
        match = re.search(r'lng:(\d+\.\d+),lat:(\d+\.\d+)', html_body)
        if match:
            lng = float(match.group(1))
            lat = float(match.group(2))

        # addr title mean_price cover_image

        tmp = tree_node.xpath('//div[@id="basic-info"]/h1[@class="shop-name"]/text()')
        title = None
        if tmp:
            title = tmp[0].strip()
        if not title:
            return

        city_info = context['city_info']
        m = {'city_id': city_info['city_id'], 'city_name': city_info['city_name'],
             'city_pinyin': city_info['city_pinyin'],
             'shop_id': shop_id,
             'taste_rating': taste_rating,
             'env_rating': env_rating,
             'service_rating': service_rating,
             'tel': tel, 'open_time': open_time, 'desc': desc, 'dishes': dishes,
             'tags': list(tags) if tags else None,
             'lat': lat, 'lng': lng,
             'title': title, 'addr': addr, 'cover_image': cover, 'mean_price': mean_price,
             'review_stat': self.parse_review_stat(shop_id)}
        return m

    def parse_review_stat(self, shop_id):
        """
        解析评论统计
        """
        template = 'http://www.dianping.com/ajax/json/shop/wizard/getReviewListFPAjax?' \
                   'act=getreviewfilters&shopId=%d&tab=all'
        review_url = template % shop_id

        redis_key = 'dianping:shop_review_%d' % shop_id
        response_body = self.engine.redis.get(redis_key)
        try:
            if not response_body:
                response = self.request.get(url=review_url)
                data = json.loads(response.text)
                self.engine.redis.set(redis_key, response.text)
                return data['msg']
            else:
                data = json.loads(response_body)
                return data['msg']
        except ValueError:
            return


    @staticmethod
    def json_validator(response):
        try:
            data = json.loads(response.text)
            return data['code'] == 200 and 'msg' in data
        except (ValueError, KeyError):
            return False

    def parse_search_list(self, response, context):
        """
        解析搜索结果列表，返回shop details
        """
        from lxml import etree

        tree_node = etree.fromstring(response.text, parser=etree.HTMLParser())

        # pagination
        pages = []
        for page_text in tree_node.xpath('//div[@class="page"]/a[@href and @data-ga-page]/@data-ga-page'):
            try:
                pages.append(int(page_text))
            except ValueError:
                continue
        if pages:
            max_page = max(pages)
        else:
            max_page = 0

        for shop in self.parse_shop_page(response, context):
            yield shop

        # TODO 暂时取消分页
        max_page = 0
        for page_idx in xrange(2, max_page + 1):
            page_url = response.url + '/p%d' % page_idx
            page_response = self.request.get(page_url)

            for shop in self.parse_shop_page(page_response, context):
                yield shop

    @staticmethod
    def default_validator(response):
        """
        默认的response验证器，通过判断HTTP status code来确定代理是否有效

        :param response:
        :return:
        """
        return response.status_code in [200, 301, 302, 304, 404]

    @staticmethod
    def store_shops(shop_list):
        """
        将shop保存到raw_dianping数据库
        """
        col = get_mongodb('raw_dianping', 'Dining', 'mongo-raw')
        for shop in shop_list:
            ret = col.find_one({'shop_id': shop['shop_id']}, {'_id': 1})
            if not ret:
                col.update({'shop_id': shop['shop_id']}, {'$set': shop}, upsert=True)

    def dianping_match(self, entry):
        """
        进行match操作
        """
        city_info = self.city_map[entry['locality']['_id']]
        city_id = city_info['city_id']
        shop_name = entry['zhName']

        context = {'city_info': city_info, 'shop_name': shop_name}

        url = 'http://www.dianping.com/search/keyword/%d/0_%s' % (city_id, shop_name)
        search_response = self.request.get(url, user_data={'ProxyMiddleware': {'validator': self.default_validator}})
        if search_response.status_code == 404:
            return

        shop_list = list(self.parse_search_list(search_response, context))
        if not shop_list:
            return

        self.store_shops(shop_list)

        the_shop = shop_list[0]
        # 检查经纬度是否一致
        try:
            coords1 = entry['location']['coordinates']
            coords2 = [the_shop['lng'], the_shop['lat']]
        except KeyError:
            return

        from utils import haversine

        # 最多允许1km的误差
        max_distance = 1
        try:
            if haversine(coords1[0], coords1[1], coords2[0], coords2[1]) < max_distance:
                self.bind_shop_id(entry, the_shop['shop_id'])
        except TypeError:
            self.log('Unable to locate shop: %d' % the_shop['shop_id'], logging.WARN)

    @staticmethod
    def bind_shop_id(shop, dianping_id):
        col = get_mongodb('poi', 'Restaurant', 'mongo')
        col.update({'_id': shop['_id']}, {'$set': {'source.dianping': {'id': dianping_id}}})

    def populate_tasks(self):
        for val in self.build_cursor():
            def task(entry=val):
                self.dianping_match(entry)

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
        parser.add_argument('--query', type=str)
        self.args, leftover = parser.parse_known_args()

    def build_cursor(self):
        col = get_mongodb('raw_dianping', 'Dining', 'mongo-raw')

        query = {}
        if self.args.query:
            exec 'from bson import ObjectId'
            query = eval(self.args.query)

        cursor = col.find(query).skip(self.args.skip)
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
                    if not isinstance(lat, float) or not isinstance(lng, float):
                        return
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
        if 'review_stat' not in entry:
            return

        review = entry['review_stat']
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
        col_im = get_mongodb('imagestore', 'Images', 'mongo')
        if not col_im.find_one({'key': image['key']}, {'_id': 1}):
            col = get_mongodb('imagestore', 'ImageCandidates', 'mongo')
            col.update({'key': image['key']}, {'$set': image}, upsert=True)
        return image['key']

    @staticmethod
    def update_shop(shop):
        """
        将店铺存储至数据库
        """
        if 'cover_image' in shop:
            cover = shop.pop('cover_image')
            image_key = DianpingProcessor.add_image(cover)
            shop['images'] = [{'key': image_key}]

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