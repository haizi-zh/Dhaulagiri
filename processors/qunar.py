# coding=utf-8
from hashlib import md5
import re

import gevent
import pymongo

from utils.database import get_mongodb, get_mysql_db
from processors import BaseProcessor, runproc


__author__ = 'zephyre'


class QunarPoiProcessor(BaseProcessor):
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
        data['rating'] = data['hotness']

        col_im = get_mongodb('raw_qunar', 'Image', profile='mongo-raw')
        images = []
        for img in col_im.find({'poi_id': poi_id}).sort('ord', pymongo.ASCENDING).limit(10):
            images.append({'key': md5(img['url']).hexdigest()})
        if images:
            data['images'] = images

        return data

    @runproc
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


class QunarCommentProcessor(BaseProcessor):
    name = 'qunar-comment'

    def __init__(self):
        super(QunarCommentProcessor, self).__init__()

        self.args = self.args_builder()

    @staticmethod
    def args_builder():
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument('--limit', default=None, type=int)
        parser.add_argument('--skip', default=0, type=int)
        args, leftovers = parser.parse_known_args()
        return args

    def run(self):
        col = get_mongodb('raw_qunar', 'Comment', 'mongo-raw')
        col_shopping = get_mongodb('poi', 'Shopping', 'mongo')
        col_dining = get_mongodb('poi', 'Restaurant', 'mongo')
        col_cmt = get_mongodb('misc', 'Comment', 'mongo')

        cursor = col.find({})
        if self.args.limit:
            cursor.limit(self.args.limit)
        if self.args.skip:
            cursor.skip(self.args.skip)

        self.total = cursor.count(with_limit_and_skip=True)

        poi_cache = {'dining': {}, 'shopping': {}}

        super(QunarCommentProcessor, self).run()

        for entry in cursor:
            def func(val=entry):

                self.progress += 1

                poi_id = val['poi_id']
                poi_type = val['poi_type']
                cmt_id = val['comment_id']

                if poi_id not in poi_cache[poi_type]:
                    the_col = {'dining': col_dining, 'shopping': col_shopping}[poi_type]
                    ret = the_col.find_one({'source.qunar.id': poi_id}, {'_id': 1})
                    if ret:
                        poi_cache[poi_type][poi_id] = ret['_id']
                    else:
                        return

                item_id = poi_cache[poi_type][poi_id]
                data = {'source': {'qunar': {'id': cmt_id}}, 'itemId': item_id,
                        'publishTime': long(1420727777000), 'type': poi_type, 'contents': val['contents']}
                if 'rating' in val and val['rating']:
                    data['rating'] = val['rating']

                meta = {}
                if 'user_name' in val:
                    meta['userName'] = val['user_name']
                if meta:
                    data['meta'] = meta

                print ('Upserting: %s' % val['title']).encode('utf-8')
                col_cmt.update({'source.qunar.id': cmt_id}, {'$set': data}, upsert=True)

            self.add_task(func)
            gevent.sleep(0)

        self._join()


class QunarImageSpider(BaseProcessor):
    """
    调用http://travel.qunar.com/place/api/poi/image?offset=0&limit=1000&poiId=3202964接口，
    补全去哪儿POI的图像信息
    """

    name = 'qunar-image-spider'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)
        self.args = self.args_builder()

    def args_builder(self):
        parser = self.arg_parser
        args, leftover = parser.parse_known_args()
        return args

    @staticmethod
    def validator(response):
        if response.status_code != 200 or 'security.qunar.com' in response.url:
            return False
        else:
            return True

    def populate_tasks(self):
        col = get_mongodb('poi', 'Restaurant', 'mongo')
        col_im = get_mongodb('imagestore', 'Images', 'mongo')
        col_cand = get_mongodb('imagestore', 'ImageCandidates', 'mongo')

        cursor = col.find({'source.qunar.id': {'$ne': None}}, {'source.qunar.id'})

        for val in cursor:
            def func(entry=val):
                qunar_id = entry['source']['qunar']['id']
                url = 'http://travel.qunar.com/place/api/poi/image?offset=0&limit=1000&poiId=%d' % qunar_id
                response = self.request.get(url, user_data={'ProxyMiddleware': {'validator': self.validator}})

                self.logger.info('Processing poi: %d, url: %s' % (qunar_id, url))

                for idx, img_entry in enumerate(response.json()['data']):
                    url = img_entry['url']
                    key = md5(url).hexdigest()
                    url_hash = key
                    ord = idx

                    image = {'url': url, 'key': key, 'url_hash': url_hash, 'ord': ord}

                    if img_entry['userName']:
                        image['meta'] = {'userName': img_entry['userName']}

                    self.logger.info('Retrieved image: %s, url=%s, poi=%d' % (key, url, qunar_id))
                    ops = {'$set': image, '$addToSet': {'itemIds': entry['_id']}}
                    ret = col_im.update({'url_hash': url_hash}, ops)
                    if not ret['updatedExisting']:
                        col_cand.update({'url_hash': url_hash}, ops, upsert=True)

            self.add_task(func)


class QunarImageProcessor(BaseProcessor):
    name = 'qunar-image'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)
        self.args = self.args_builder()

    def args_builder(self):
        parser = self.arg_parser
        parser.add_argument('--limit', default=None, type=int)
        parser.add_argument('--skip', default=0, type=int)
        args, leftover = parser.parse_known_args()
        return args

    def populate_tasks(self):
        col = get_mongodb('raw_qunar', 'Image', 'mongo-raw')
        col_shopping = get_mongodb('poi', 'Shopping', 'mongo')
        col_dining = get_mongodb('poi', 'Restaurant', 'mongo')
        col_cand = get_mongodb('imagestore', 'ImageCandidates', 'mongo')
        col_img = get_mongodb('imagestore', 'Images', 'mongo')

        cursor = col.find({})
        if self.args.limit:
            cursor.limit(self.args.limit)
        if self.args.skip:
            cursor.skip(self.args.skip)

        poi_cache = {'dining': {}, 'shopping': {}}

        for entry in cursor:
            def func(val=entry):

                if 'poi_id' not in val:
                    return

                poi_id = val['poi_id']
                poi_type = val['poi_type']

                if poi_id not in poi_cache[poi_type]:
                    the_col = {'dining': col_dining, 'shopping': col_shopping}[poi_type]
                    ret = the_col.find_one({'source.qunar.id': poi_id}, {'_id': 1})
                    if ret:
                        poi_cache[poi_type][poi_id] = ret['_id']
                    else:
                        return

                item_id = poi_cache[poi_type][poi_id]

                url = val['url']
                key = md5(url).hexdigest()
                url_hash = key
                data = {'itemIds': [item_id], 'url': url, 'key': key, 'url_hash': url_hash}
                if 'ord' in val:
                    data['ord'] = val['ord']

                meta = {}
                if 'user_name' in val:
                    meta['userName'] = val['user_name']
                if meta:
                    data['meta'] = meta

                ret = col_img.update({'url_hash': url_hash}, {'$set': data})
                if not ret['updatedExisting']:
                    self.log('Added to candidates: %s' % url_hash)
                    col_cand.update({'url_hash': url_hash}, {'$set': data}, upsert=True)

            self.add_task(func)