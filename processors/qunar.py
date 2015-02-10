# coding=utf-8
from hashlib import md5
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


class QunarCommentImport(BaseProcessor):
    """
    将去哪儿POI的评论导入到数据库中
    """

    name = 'qunar-comment-importer'

    @staticmethod
    def process_avatar(avatar):
        avatar = avatar.strip()
        if not avatar:
            return None

        from hashlib import md5

        key = md5(avatar).hexdigest()
        url_hash = key

        col_cand = get_mongodb('imagestore', 'ImageCandidates', 'mongo')
        col_im = get_mongodb('imagestore', 'Images', 'mongo')

        url = avatar
        image = {'url': url, 'key': key, 'url_hash': url_hash}

        if not col_im.find_one({'url_hash': url_hash}, {'_id'}):
            col_cand.update({'url_hash', url_hash}, {'$set': image})

        return image

    def populate_tasks(self):
        col_dining = get_mongodb('poi', 'Restaurant', 'mongo')
        col_shopping = get_mongodb('poi', 'Shopping', 'mongo')
        col_cmt = get_mongodb('raw_qunar', 'QunarPoiComment', 'mongo')

        poi_id_list = col_cmt.distinct("poi_id")

        for val in poi_id_list:
            def func(poi_id=val):
                # 查找poi_id对应的item
                item = None
                for col in [col_dining, col_shopping]:
                    item = col.find_one({'source.qunar.id': poi_id}, {'_id': 1})
                    if item:
                        break

                if not item:
                    return

                for entry in col_cmt.find({'poi_id': poi_id}):
                    comment = {'source': {'qunar': {'id': poi_id}}, 'itemId': item['_id'], 'rating': entry['rating']}
                    if 'images' in entry and entry['images']:
                        images = []
                        for img in entry['images']:
                            img_entry = self.process_avatar(img['url'])
                            if img_entry:
                                images.append(img_entry)
                        if images:
                            comment['images'] = images

                    if 'user_avatar' in entry and entry['user_avatar']:
                        img_entry = self.process_avatar(entry['user_avatar'])
                        if img_entry:
                            comment['authorAvatar'] = img_entry['key']

                    if 'user_name' in entry and entry['user_name']:
                        comment['authorName'] = entry['user_name']



        cursor = col_cmt.find(query).sort('source.qunar.id', pymongo.ASCENDING)
        if self.args.limit:
            cursor.limit(self.args.limit)
        cursor.skip(self.args.skip)


    name = 'qunar-poi-import'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)
        self.args = self.args_builder()

    def args_builder(self):
        parser = self.arg_parser
        parser.add_argument('--limit', default=None, type=int)
        parser.add_argument('--skip', default=0, type=int)
        parser.add_argument('--query', type=str)
        parser.add_argument('--type', choices=['dining', 'shopping'], required=True, type=str)
        args, leftover = parser.parse_known_args()
        return args

    @staticmethod
    def validator(response):
        if response.status_code != 200 or 'security.qunar.com' in response.url:
            return False
        try:
            response.json()['data']
        except (ValueError, KeyError):
            return False

        return True


class QunarCommentSpider(BaseProcessor):
    """
    调用http://travel.qunar.com/place/api/html/comments/poi/3202964?sortField=1&img=true&pageSize=10&page=1接口，
    抓取去哪儿POI的评论数据
    """
    name = 'qunar-poi-comment'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)
        self.args = self.args_builder()

    def args_builder(self):
        parser = self.arg_parser
        parser.add_argument('--limit', default=None, type=int)
        parser.add_argument('--skip', default=0, type=int)
        parser.add_argument('--query', type=str)
        parser.add_argument('--type', choices=['dining', 'shopping'], required=True, type=str)
        args, leftover = parser.parse_known_args()
        return args

    @staticmethod
    def validator(response):
        if response.status_code != 200 or 'security.qunar.com' in response.url:
            return False
        try:
            response.json()['data']
        except (ValueError, KeyError):
            return False

        return True

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

        comment_list = []

        node_list = []

        try:
            node_list = etree.fromstring(data, etree.HTMLParser()).xpath(
                '//ul[@id="comment_box"]/li[contains(@class,"e_comment_item")]')
        except ValueError:
            self.logger.warn(data)

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
                comment['user_avatar'] = self.resolve_avatar(avatar)

            tmp = comment_node.xpath('.//div[@class="e_comment_usr"]/div[@class="e_comment_usr_name"]/a/text()')
            if tmp and tmp[0].strip():
                comment['user_name'] = tmp[0].strip()

            comment_list.append(comment)

        return comment_list

    def populate_tasks(self):
        col_name = {'dining': 'Restaurant', 'shopping': 'Shopping'}[self.args.type]
        col = get_mongodb('poi', col_name, 'mongo')
        col_raw = get_mongodb('raw_qunar', 'QunarPoiComment', 'mongo-raw')

        query = {'source.qunar.id': {'$ne': None}}
        extra_query = eval(self.args.query) if self.args.query else {}
        if extra_query:
            query = {'$and': [query, extra_query]}

        cursor = col.find(query, {'source.qunar.id'}).sort('source.qunar.id', pymongo.ASCENDING)
        if self.args.limit:
            cursor.limit(self.args.limit)
        cursor.skip(self.args.skip)

        tmpl = 'http://travel.qunar.com/place/api/html/comments/poi/%d?sortField=1&pageSize=%d&page=%d'

        for val in cursor:
            def func(entry=val):
                qunar_id = entry['source']['qunar']['id']

                page = 1
                page_size = 50
                while True:
                    url = tmpl % (qunar_id, page_size, page)
                    self.logger.info('Retrieving: poi: %d, page: %d, url: %s' % (qunar_id, page, url))

                    try:
                        response = self.request.get(url, timeout=15,
                                                    user_data={'ProxyMiddleware': {'validator': self.validator}})
                    except IOError:
                        self.logger.warn('Failed to read %s due to IOError' % url)
                        break

                    data = response.json()
                    if data['errmsg'] != 'success':
                        self.logger.warn('Error while retrieving %s, errmsg: %s' % (url, data['errmsg']))
                        break

                    comments = self.parse_comments(response.json()['data'])
                    for c in comments:
                        c['poi_id'] = qunar_id
                        col_raw.update({'comment_id': c['comment_id']}, {'$set': c}, upsert=True)

                    # 如果返回空列表，或者comments数量不足pageSize，说明已经到达最末页
                    if not comments or len(comments) < page_size:
                        break

                    page += 1
                    continue

            setattr(func, 'task_key', '%s:%d' % (self.name, val['source']['qunar']['id']))
            self.add_task(func)


class QunarImageImporter(BaseProcessor):
    """
    设置好去哪儿POI的图像
    """

    name = 'qunar-image-importer'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)
        self.args = self.args_builder()

    def args_builder(self):
        parser = self.arg_parser
        parser.add_argument('--limit', default=None, type=int)
        parser.add_argument('--skip', default=0, type=int)
        parser.add_argument('--type', choices=['dining', 'shopping'], required=True, type=str)
        args, leftover = parser.parse_known_args()
        return args

    def populate_tasks(self):
        col_name = {'dining': 'Restaurant', 'shopping': 'Shopping'}[self.args.type]
        col = get_mongodb('poi', col_name, 'mongo')
        col_im = get_mongodb('imagestore', 'Images', 'mongo')

        cursor = col.find({}, {'_id': True})
        if self.args.limit:
            cursor.limit(self.args.limit)
        cursor.skip(self.args.skip)
        cursor.sort('hotness', pymongo.DESCENDING)

        for entry in cursor:
            oid = entry['_id']

            def func(entry_id=oid):
                max_images_cnt = 5
                ret = list(
                    col_im.find({'itemIds': entry_id}, {'key': 1, 'meta': 1}).sort('ord', pymongo.ASCENDING).limit(
                        max_images_cnt))
                if ret:
                    images = []
                    for tmp in ret:
                        del tmp['_id']
                        images.append(tmp)
                    col.update({'_id': entry_id}, {'$set': {'images': images}})

            setattr(func, 'task_key', 'qunar-image-importer-%s' % oid)
            self.add_task(func)


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
        parser.add_argument('--limit', default=None, type=int)
        parser.add_argument('--skip', default=0, type=int)
        parser.add_argument('--query', type=str)
        parser.add_argument('--type', choices=['dining', 'shopping'], required=True, type=str)
        args, leftover = parser.parse_known_args()
        return args

    @staticmethod
    def validator(response):
        if response.status_code != 200 or 'security.qunar.com' in response.url:
            return False
        try:
            response.json()['data']
        except (ValueError, KeyError):
            return False

        return True

    def populate_tasks(self):
        col_name = {'dining': 'Restaurant', 'shopping': 'Shopping'}[self.args.type]
        col = get_mongodb('poi', col_name, 'mongo')

        col_im = get_mongodb('imagestore', 'Images', 'mongo')
        col_cand = get_mongodb('imagestore', 'ImageCandidates', 'mongo')

        col_raw = get_mongodb('raw_qunar', 'QunarPoiImage', 'mongo-raw')

        query = {'source.qunar.id': {'$ne': None}}
        extra_query = eval(self.args.query) if self.args.query else {}
        if extra_query:
            query = {'$and': [query, extra_query]}

        cursor = col.find(query, {'source.qunar.id': 1}).sort('source.qunar.id', pymongo.ASCENDING)
        if self.args.limit:
            cursor.limit(self.args.limit)
        cursor.skip(self.args.skip)

        for val in cursor:
            def func(entry=val):
                qunar_id = entry['source']['qunar']['id']

                # 在数据库中查询
                ret = col_raw.find_one({'id': qunar_id}, {'data': 1})
                if not ret:
                    url = 'http://travel.qunar.com/place/api/poi/image?offset=0&limit=1000&poiId=%d' % qunar_id
                    self.logger.info('Processing poi: %d, url: %s' % (qunar_id, url))

                    try:
                        response = self.request.get(url, user_data={'ProxyMiddleware': {'validator': self.validator}})
                    except IOError:
                        self.logger.warn('Failed to read %s due to IOError' % url)
                        return

                    if not response:
                        self.logger.warn('Failed to read %s' % url)
                        return

                    data = response.json()['data']
                    col_raw.update({'id': qunar_id}, {'$set': {'id': qunar_id, 'data': data}}, upsert=True)
                else:
                    data = ret['data']

                for idx, img_entry in enumerate(data):
                    url = img_entry['url']
                    key = md5(url).hexdigest()
                    url_hash = key
                    ord_idx = idx

                    image = {'url': url, 'key': key, 'url_hash': url_hash, 'ord': ord_idx}

                    if img_entry['userName']:
                        image['meta'] = {'userName': img_entry['userName']}

                    self.logger.info('Retrieved image: %s, url=%s, poi=%d' % (key, url, qunar_id))
                    ops = {'$set': image, '$addToSet': {'itemIds': entry['_id']}}
                    ret = col_im.update({'url_hash': url_hash}, ops)
                    if not ret['updatedExisting']:
                        col_cand.update({'url_hash': url_hash}, ops, upsert=True)

            self.add_task(func)