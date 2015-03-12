# coding=utf-8
from hashlib import md5
import json
import re

import pymongo

from processors import BaseProcessor
from utils.database import get_mongodb, get_mysql_db


__author__ = 'zephyre'


class QunarPoiProcessor(BaseProcessor):
    def populate_tasks(self):
        pass

    name = 'qunar-poi'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)

        self.args = self.args_builder()
        self.conn = None
        self.denom = None

    def args_builder(self):
        parser = self.arg_parser
        parser.add_argument('--limit', default=None, type=int)
        parser.add_argument('--skip', default=0, type=int)
        parser.add_argument('--cat', required=True, choices=['dining', 'shopping', 'hotel'], type=str)
        parser.add_argument('--query', type=str)
        parser.add_argument('--order', type=str)
        return parser.parse_args()

    @staticmethod
    def haversine(lon1, lat1, lon2, lat2):
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)
        """
        from math import radians, sin, cos, asin, sqrt
        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))

        # 6367 km is the radius of the Earth
        km = 6367 * c
        return km

    def build_poi(self, entry, poi_type):
        poi_id = int(entry['id'])
        data = {'zhName': entry['name'], 'source': {'qunar': {'id': poi_id}},
                'location': {'type': 'Point', 'coordinates': [float(entry[key]) for key in ['lng', 'lat']]}}

        if entry['priceDesc']:
            try:
                price = int(entry['priceDesc'])
                data['price'] = price
            except ValueError:
                data['priceDesc'] = entry['priceDesc']

        for k1, k2 in [['addr', 'address'], ['tel'] * 2]:
            if entry[k1]:
                data[k2] = entry[k1]

        data['alias'] = [data['zhName'].lower()]

        col_country = get_mongodb('geo', 'Country', profile='mongo')
        ret = col_country.find_one({'alias': entry['countryName'].lower().strip()}, {'zhName': 1, 'enName': 1})
        assert ret is not None, 'Cannot find country: %s' % entry['countryName']
        data['country'] = ret

        col_loc = get_mongodb('geo', 'Locality', profile='mongo')
        ret = col_loc.find_one({'alias': re.compile(ur'^%s' % entry['distName'].lower().strip())},
                               {'zhName': 1, 'enName': 1, 'location': 1})

        coord1 = data['location']['coordinates']
        coord2 = ret['location']['coordinates']
        dist = self.haversine(coord1[0], coord1[1], coord2[0], coord2[1])
        if dist >= 300:
            print ('Cannot find city: %s' % entry['distName']).encode('utf-8')
            return
        data['locality'] = ret

        data['targets'] = [data['country']['_id'], data['locality']['_id']]

        if entry['tag']:
            data['tags'] = filter(lambda val: val, re.split(r'\s+', entry['tag']))

        if entry['intro']:
            data['desc'] = entry['intro']

        if poi_type in ['dining', 'shopping']:
            for k1, k2 in [['style'] * 2, ['openTime'] * 2]:
                if entry[k1]:
                    data[k2] = entry[k1]

            if entry['special']:
                data['specials'] = filter(lambda val: val, re.split(r'\s+', entry['special']))

        cursor = self.conn.cursor()
        cursor.execute('SELECT COUNT(*) AS cnt FROM qunar_%s WHERE hotScore<%d' % (
            'meishi' if poi_type == 'dining' else 'gouwu', entry['hotScore']))
        data['hotness'] = float(cursor.fetchone()['cnt']) / self.denom
        # TODO rating和hotness不能一样
        data['rating'] = data['hotness']

        col_im = get_mongodb('raw_qunar', 'Image', profile='mongo-raw')
        images = []
        for img in col_im.find({'poi_id': poi_id}).sort('ord', pymongo.ASCENDING).limit(10):
            images.append({'key': md5(img['url']).hexdigest()})
        if images:
            data['images'] = images

        return data

    def run(self):
        self.conn = get_mysql_db('restore_poi', profile='mysql')

        table = {'dining': 'qunar_meishi', 'shopping': 'qunar_gouwu', 'hotel': 'qunar_jiudian'}[self.args.cat]

        stmt_tmpl = 'SELECT * FROM %s' % table
        order = 'ORDER BY %s' % self.args.order if self.args.order else ''
        query = 'WHERE %s' % self.args.query if self.args.query else ''

        import sys

        if self.args.limit or self.args.skip:
            limit = self.args.limit if self.args.limit else sys.maxint
            offset = self.args.skip
        else:
            limit = sys.maxint
            offset = 0

        cur = self.conn.cursor()
        cur.execute('SELECT COUNT(*) AS cnt FROM %s' % table)
        self.denom = cur.fetchone()['cnt']

        batch_size = 5
        start = offset

        while True:
            if start > offset + limit:
                break

            l = batch_size
            if start + l > offset + limit:
                l = offset + limit - start

            tail = ' LIMIT %d, %d' % (start, l)
            start += l

            stmt = '%s %s %s %s' % (stmt_tmpl, query, order, tail)
            cur.execute(stmt)

            if cur.rowcount < 1:
                break

            for entry in cur:
                def func(val=entry):
                    print ('Upserting %s' % val['name']).encode('utf-8')
                    data = self.build_poi(val, self.args.cat)
                    if not data:
                        return

                    col_name = {'dining': 'Restaurant', 'shopping': 'Shopping', 'hotel': 'Hotel'}[self.args.cat]
                    col = get_mongodb('poi', col_name, profile='mongo')
                    col.update({'source.qunar.id': data['source']['qunar']['id']}, {'$set': data}, upsert=True)
                    self.progress += 1

                self.add_task(func)


def status_code_validator(response, allowed_codes):
    return response.status_code in allowed_codes


def security_validator(response):
    return 'security.qunar.com' not in response.url


def qunar_validator(response):
    """
    验证返回结果，处理去哪儿限制爬虫的情况
    """
    return status_code_validator(response, [200]) and security_validator(response)


def qunar_size_validator(response, min_size=None, max_size=None):
    """
    根据response的size进行验证
    """
    sz = len(response.text)
    if min_size and sz < min_size:
        return False
    if max_size and sz > max_size:
        return False
    return True


def qunar_json_validator(response):
    try:
        response.json()['data']
    except (ValueError, KeyError):
        return False

    return True


class QunarFetcher(BaseProcessor):
    """
    根据去哪儿的POI数据，补充相应的信息
    """

    name = 'qunar.fetch'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)
        self.args = self.args_builder()

    def get_action(self):
        """
        根据参数，获得action对象
        """
        parser = self.arg_parser
        parser.add_argument('--action', type=str, required=True)
        args, leftover = parser.parse_known_args()
        self.args = args

        context = {
            'limit': self.args.limit,
            'skip': self.args.skip,
            'query': self.args.query,
            'type': self.args.type
        }

        fetcher = self
        actions = {
            'comment-spider': QunarCommentSpider(fetcher, context),
            'poi-spider': QunarPoiSpider(fetcher, context),
            'image-spider': QunarImageSpider(fetcher, context),
            'comment-proc': QunarCommentProcessor(fetcher, context)
        }
        action_name = self.args.action
        if action_name not in actions:
            self.logger.critical('Unknown action name: %s' % action_name)
            return
        else:
            return actions[action_name]

    def args_builder(self):
        parser = self.arg_parser
        parser.add_argument('--limit', default=None, type=int)
        parser.add_argument('--skip', default=0, type=int)
        parser.add_argument('--query', type=str)
        parser.add_argument('--type', choices=['dining', 'shopping'], required=True, type=str)
        args, leftover = parser.parse_known_args()
        return args

    def populate_tasks(self):
        action = self.get_action()
        cursor = action.build_cursor()

        for val in cursor:
            def func(entry=val):
                action.process(entry)

            setattr(func, 'task_key', 'task:qunar.fetch:%s:%s' % (self.args.action, val['_id']))
            self.add_task(func)


class QunarPoiSpider(object):
    """
    补充去哪儿POI的数据（主要是评分）
    """

    def __init__(self, fetcher, context):
        self.fetcher = fetcher
        self.request = fetcher.request
        self.logger = fetcher.logger
        self.redis = fetcher.engine.redis_cli
        self.context = context

    def build_cursor(self):
        col_name = {'dining': 'Restaurant', 'shopping': 'Shopping'}[self.context['type']]
        col = get_mongodb('poi', col_name, 'mongo')
        query = {'source.qunar.id': {'$ne': None}}
        if self.context['query']:
            exec 'from bson import ObjectId'
            query = {'$and': [query, eval(self.context['query'])]}
        cursor = col.find(query, {'source.qunar.id': 1}).sort('source.qunar.id', pymongo.ASCENDING)
        if self.context['limit']:
            cursor.limit(self.context['limit'])
        cursor.skip(self.context['skip'])
        return cursor

    def process(self, entry):
        qunar_id = entry['source']['qunar']['id']
        poi_url = 'http://travel.qunar.com/p-oi%d' % qunar_id

        def val_func(v):
            min_size = 4 * 1024
            return qunar_size_validator(v, min_size) and security_validator(v) and status_code_validator(v, [200, 404])

        response = self.request.get(poi_url, timeout=15, user_data={'ProxyMiddleware': {'validator': val_func}})
        if response.status_code == 404:
            return

        from lxml import etree

        tree_node = etree.fromstring(response.text, parser=etree.HTMLParser())
        try:
            score_text = tree_node.xpath('//div[@class="scorebox clrfix"]/span[@class="cur_score"]/text()')[0]
            score = float(score_text)
        except (IndexError, ValueError):
            self.logger.warn('Failed to get rating: %s' % poi_url)
            return

        if score > 0:
            col_name = {'dining': 'Restaurant', 'shopping': 'Shopping'}[self.context['type']]
            col = get_mongodb('poi', col_name, 'mongo')
            col.update({'_id': entry['_id']}, {'$set': {'rating': score}})


class QunarCommentSpider(object):
    """
    调用http://travel.qunar.com/place/api/html/comments/poi/3202964?sortField=1&img=true&pageSize=10&page=1接口，
    抓取去哪儿POI的评论数据
    """

    def __init__(self, fetcher, context):
        self.fetcher = fetcher
        self.request = fetcher.request
        self.logger = fetcher.logger
        self.redis = fetcher.engine.redis_cli
        self.context = context

    def resolve_avatar(self, url):
        """
        根据url获得最终的链接地址（处理重定向问题）
        """
        ret_url = url
        response = self.request.get(url, allow_redirects=False)
        if response.status_code in [301, 302]:
            try:
                ret_url = response.headers['location']
            except KeyError:
                pass

        return ret_url

    def parse_comments(self, data):
        from lxml import etree
        from datetime import datetime, timedelta
        from hashlib import md5

        try:
            node_list = etree.fromstring(data, etree.HTMLParser()).xpath(
                '//ul[@id="comment_box"]/li[contains(@class,"e_comment_item")]')
        except ValueError:
            self.logger.warn(data)
            return

        for comment_node in node_list:
            comment = {'comment_id': int(re.search(r'cmt_item_(\d+)', comment_node.xpath('./@id')[0]).group(1))}

            for k1, k2 in [['title', 'e_comment_title'], ['contents', 'e_comment_content']]:
                tmp = comment_node.xpath('.//div[@class="%s"]' % k2)
                if tmp:
                    tmp = tmp[0]
                    text = ''.join(tmp.itertext())
                    if text:
                        comment[k1] = text

            tmp = comment_node.xpath('.//div[@class="e_comment_star_box"]//span[contains(@class,"cur_star")]/@class')
            if tmp:
                match = re.search(r'star_(\d)', tmp[0])
                if match:
                    comment['rating'] = float(match.group(1)) / 5.0

            images = []
            for image_node in comment_node.xpath('.//div[@class="e_comment_imgs_box"]'
                                                 '//a[@data-beacon="comment_pic"]/img[@src]'):
                tmp = image_node.xpath('./@src')
                if not tmp:
                    continue
                images.append({'url': re.sub(r'_r_\d+x\d+[^/]+\.jpg', '', tmp[0])})

            if images:
                comment['images'] = images

            for tmp in comment_node.xpath('.//div[@class="e_comment_add_info"]/ul/li/text()'):
                try:
                    comment['cTime'] = long((datetime.strptime(tmp, '%Y-%m-%d') -
                                             datetime.utcfromtimestamp(0) - timedelta(hours=8)).total_seconds())
                    break
                except ValueError:
                    pass

            tmp = comment_node.xpath('.//div[@class="e_comment_usr"]/div[@class="e_comment_usr_pic"]/a/img[@src]/@src')
            if tmp:
                avatar = re.sub(r'\?\w$', '', tmp[0])

                redis_key = 'qunar:poi-comment:avatar:%s' % md5(avatar).hexdigest()
                avatar_expire = 7 * 24 * 3600
                comment['user_avatar'] = self.redis.get_cache(redis_key, lambda: self.resolve_avatar(avatar),
                                                              expire=avatar_expire)

            tmp = comment_node.xpath('.//div[@class="e_comment_usr"]/div[@class="e_comment_usr_name"]/a/text()')
            if tmp and tmp[0].strip():
                comment['user_name'] = tmp[0].strip()

            yield comment

    def build_cursor(self):
        col_name = {'dining': 'Restaurant', 'shopping': 'Shopping'}[self.context['type']]
        col = get_mongodb('poi', col_name, 'mongo')
        query = {'source.qunar.id': {'$ne': None}}
        if self.context['query']:
            exec 'from bson import ObjectId'
            query = {'$and': [query, eval(self.context['query'])]}
        cursor = col.find(query, {'source.qunar.id': 1}).sort('source.qunar.id', pymongo.ASCENDING)
        if self.context['limit']:
            cursor.limit(self.context['limit'])
        cursor.skip(self.context['skip'])
        return cursor

    def process(self, entry):
        """
        开始处理评论列表
        """
        col_raw = get_mongodb('raw_qunar', 'PoiComment', 'mongo-raw')
        tmpl = 'http://travel.qunar.com/place/api/html/comments/poi/%d?sortField=1&pageSize=%d&page=%d'
        qunar_id = entry['source']['qunar']['id']

        page = 0
        page_size = 50

        while True:
            page += 1
            comments_list_url = tmpl % (qunar_id, page_size, page)
            self.logger.debug('Fetching: poi: %d, page: %d, url: %s' % (qunar_id, page, comments_list_url))

            redis_key = 'qunar:poi-comment:list:%d:%d:%d' % (qunar_id, page_size, page)

            def get_comments_list():
                """
                获得评论列表的response body
                """
                validators = [qunar_validator, qunar_json_validator]
                response = self.request.get(comments_list_url, timeout=15,
                                            user_data={'ProxyMiddleware': {'validator': validators}})
                return response.text

            try:
                comments_list_expire = 3600 * 24
                search_result_text = self.redis.get_cache(redis_key, get_comments_list, expire=comments_list_expire)
                data = json.loads(search_result_text)
            except (IOError, ValueError):
                self.logger.warn('Fetching failed: %s' % comments_list_url)
                break

            if data['errmsg'] != 'success':
                self.logger.warn('Fetching failed %s, errmsg: %s' % (comments_list_url, data['errmsg']))
                break

            tmp = self.parse_comments(data['data'])
            comments = list(tmp) if tmp else []
            for c in comments:
                c['poi_id'] = qunar_id
                col_raw.update({'comment_id': c['comment_id']}, {'$set': c}, upsert=True)

            # 如果返回空列表，或者comments数量不足pageSize，说明已经到达最末页
            if not comments or len(comments) < page_size:
                return


class QunarImageSpider(object):
    """
    调用http://travel.qunar.com/place/api/poi/image?offset=0&limit=1000&poiId=3202964接口，
    补全去哪儿POI的图像信息
    """

    def __init__(self, fetcher, context):
        self.fetcher = fetcher
        self.request = fetcher.request
        self.logger = fetcher.logger
        self.redis = fetcher.engine.redis_cli
        self.context = context

    def build_cursor(self):
        col_name = {'dining': 'Restaurant', 'shopping': 'Shopping'}[self.context['type']]
        col = get_mongodb('poi', col_name, 'mongo')
        query = {'source.qunar.id': {'$ne': None}}
        if self.context['query']:
            exec 'from bson import ObjectId'
            query = {'$and': [query, eval(self.context['query'])]}
        cursor = col.find(query, {'source.qunar.id': 1}).sort('source.qunar.id', pymongo.ASCENDING)
        if self.context['limit']:
            cursor.limit(self.context['limit'])
        cursor.skip(self.context['skip'])
        return cursor

    def process(self, entry):
        qunar_id = entry['source']['qunar']['id']

        # 在redis中查询
        redis_key = 'qunar:poi-image:%d' % qunar_id

        def get_poi_images():
            image_list_url = 'http://travel.qunar.com/place/api/poi/image?offset=0&limit=1000&poiId=%d' % qunar_id
            self.logger.debug('Processing poi: %d, url: %s' % (qunar_id, image_list_url))

            try:
                validators = [qunar_validator, qunar_json_validator]
                response = self.request.get(image_list_url,
                                            user_data={'ProxyMiddleware': {'validator': validators}})
            except IOError as e:
                self.logger.warn('IOError: %s' % image_list_url)
                raise e

            if not response:
                self.logger.warn('IOError: %s' % image_list_url)
                raise IOError

            return json.dumps(response.json()['data'])

        images_expire = 24 * 3600
        images = json.loads(self.redis.get_cache(redis_key, get_poi_images, expire=images_expire))
        if not images:
            return

        col_im = get_mongodb('imagestore', 'Images', 'mongo')
        col_cand = get_mongodb('imagestore', 'ImageCandidates', 'mongo')

        for idx, img_entry in enumerate(images):
            url = img_entry['url']
            key = md5(url).hexdigest()
            url_hash = key
            ord_idx = idx

            image = {'url': url, 'key': key, 'url_hash': url_hash, 'ord': ord_idx}

            if img_entry['userName']:
                image['meta'] = {'userName': img_entry['userName']}

            self.logger.debug('Retrieved image: %s, url=%s, poi=%d' % (key, url, qunar_id))
            ops = {'$set': image, '$addToSet': {'itemIds': entry['_id']}}
            ret = col_im.update({'url_hash': url_hash}, ops)
            if not ret['updatedExisting']:
                col_cand.update({'url_hash': url_hash}, ops, upsert=True)


class QunarCommentProcessor(object):
    """
    导入去哪儿POI的评论信息
    """

    def __init__(self, fetcher, context):
        self.fetcher = fetcher
        self.request = fetcher.request
        self.logger = fetcher.logger
        self.redis = fetcher.engine.redis_cli
        self.context = context

    def build_cursor(self):
        col_name = {'dining': 'Restaurant', 'shopping': 'Shopping'}[self.context['type']]
        col = get_mongodb('poi', col_name, 'mongo')
        query = {'source.qunar.id': {'$ne': None}}
        if self.context['query']:
            exec 'from bson import ObjectId'
            query = {'$and': [query, eval(self.context['query'])]}
        cursor = col.find(query, {'source.qunar.id': 1}).sort('source.qunar.id', pymongo.ASCENDING)
        if self.context['limit']:
            cursor.limit(self.context['limit'])
        cursor.skip(self.context['skip'])
        return cursor

    @staticmethod
    def update_images(comment):
        """
        根据原始的聊天，处理其中的图像
        """
        images = [comment['user_avatar']]
        if 'images' in comment:
            images_list = comment['images']
            if images_list:
                for img_item in images_list:
                    images.append(img_item['url'])

        col_im = get_mongodb('imagestore', 'Images', 'mongo')
        col_cand = get_mongodb('imagestore', 'ImageCandidates', 'mongo')
        for image_url in images:
            image_key = md5(image_url).hexdigest()
            if not col_im.find_one({'key': image_key}, {'_id': 1}):
                image_entry = {'key': image_key, 'url_hash': image_key, 'url': image_url}
                col_cand.update({'key': image_key}, {'$set': image_entry}, upsert=True)

    def process(self, entry):
        qunar_id = entry['source']['qunar']['id']
        col_raw = get_mongodb('raw_qunar', 'PoiComment', 'mongo-raw')

        col_name = {'dining': 'RestaurantComment', 'shopping': 'ShoppingComment'}[self.context['type']]
        col = get_mongodb('poi', col_name, 'mongo')

        for raw_comment in col_raw.find({'poi_id': qunar_id}):
            self.update_images(raw_comment)

            comment = {'source': {'qunar': {'id': raw_comment['comment_id']}},
                       'itemId': entry['_id'],
                       'authorAvatar': md5(raw_comment['user_avatar']).hexdigest(),
                       'authorName': raw_comment['user_name'],
                       'title': raw_comment['title']}
            if 'contents' in raw_comment and raw_comment['contents']:
                comment['contents'] = raw_comment['contents']
            if 'rating' in raw_comment and raw_comment['rating'] is not None:
                comment['rating'] = raw_comment['rating']
            timestamp = raw_comment['cTime']
            from math import log10

            if log10(timestamp) < 10:
                comment['publishTime'] = timestamp * 1000
            else:
                comment['publishTime'] = timestamp

            col.update({'source.qunar.id': raw_comment['comment_id']}, {'$set': comment}, upsert=True)