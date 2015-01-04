# coding=utf-8

import argparse
import gevent
import re

from mongo import get_mongodb


__author__ = 'zephyre'


class MfwImageExtractor(object):
    def __init__(self):
        from hashlib import md5

        def helper(image_id, src):
            key = md5(src).hexdigest()
            url = 'http://aizou.qiniudn.com/%s' % key

            return {'id': image_id, 'metadata': {}, 'src': src, 'url': url, 'key': key, 'url_hash': key}

        def f1(src):
            pattern = r'([^\./]+)\.\w+\.[\w\d]+\.(jpeg|bmp|png)$'
            match = re.search(pattern, src)
            if not match:
                return None
            c = match.group(1)
            ext = match.group(2)
            src = re.sub(pattern, '%s.%s' % (c, ext), src)
            return helper(c, src)

        self.extractor = [f1]

    def retrieve_image(self, src):
        for func in self.extractor:
            ret = func(src)
            if ret:
                return ret


class PoiCommentProcessor(MfwImageExtractor):
    name = 'mfw-poi-comment'

    def __init__(self):
        super(PoiCommentProcessor, self).__init__()
        self.args = self.args_builder()

    @staticmethod
    def args_builder():
        parser = argparse.ArgumentParser()
        parser.add_argument('cmd')
        parser.add_argument('--limit', default=None, type=int)
        parser.add_argument('--skip', default=0, type=int)
        return parser.parse_args()

    def run(self):
        col = get_mongodb('raw_mfw', 'MafengwoComment', 'mongo-raw')

        cursor = col.find({}, snapshot=True)
        cursor.skip(self.args.skip)
        if self.args.limit:
            cursor.limit(self.args.limit)

        print '%d documents to process...' % cursor.count(with_limit_and_skip=True)

        jobs = []
        for entry in cursor:
            jobs.append(gevent.spawn(self.parse, entry))

        gevent.joinall(jobs)


    @staticmethod
    def update(item_type, item_data):
        if item_type == 'comment':
            col = get_mongodb('misc', 'Comment', 'mongo')
            col.update({'source.mafengwo.id': item_data['source']['mafengwo']['id']}, {'$set': item_data}, upsert=True)
        elif item_type == 'image':
            col = get_mongodb('imagestore', 'ImageCandidates', 'mongo')
            col.update({'key': item_data['key']}, {'$set': item_data}, upsert=True)
        else:
            assert False, 'Invalid type: %s' % item_type


    def parse(self, entry):
        col_vs = get_mongodb('poi', 'ViewSpot', 'mongo')
        col_dining = get_mongodb('poi', 'Restaurant', 'mongo')
        col_shopping = get_mongodb('poi', 'Shopping', 'mongo')
        poi_dbs = {'vs': col_vs, 'dining': col_dining, 'shopping': col_shopping}


        def fetch_poi_item(mfw_id, poi_type):
            col_poi = poi_dbs[poi_type]
            tmp = col_poi.find_one({'source.mafengwo.id': mfw_id}, {'_id': 1})
            if tmp:
                return {'type': poi_type, 'item_id': tmp['_id']}
            else:
                return None

        ret = None
        for v in ['vs', 'dining', 'shopping']:
            ret = fetch_poi_item(entry['poi_id'], v)
            if ret:
                break

        if not ret:
            return

        for item_type, item_data in self.parse_contents(entry['contents']):
            if item_type != 'image':
                item_data['source'] = {'mafengwo': {'id': entry['comment_id']}}
                item_data['type'] = ret['type']
                item_data['itemId'] = ret['item_id']

            self.update(item_type, item_data)

    def parse_contents(self, node):
        from lxml import etree
        from datetime import datetime, timedelta

        sel = etree.fromstring(node, parser=etree.HTMLParser())
        avatar = sel.xpath('//span[@class="user-avatar"]/a[@href]/img[@src]/@src')[0]
        ret = self.retrieve_image(avatar)

        if ret:
            # 检查是否已经存在于数据库中
            col_im = get_mongodb('imagestore', 'Images', 'mongo')
            col_cand = get_mongodb('imagestore', 'ImageCandidates', 'mongo')

            img = col_im.find_one({'url_hash': ret['url_hash']}, {'_id': 1})
            if not img:
                img = col_cand.find_one({'url_hash': ret['url_hash']}, {'_id': 1})
            if not img:
                # 添加到待抓取列表中
                data = {'key': ret['key'], 'url': ret['src'], 'url_hash': ret['url_hash']}
                item_type = 'image'
                yield item_type, data

            avatar = ret['key']
        else:
            avatar = ''

        tmp = sel.xpath('//div[@class="info"]/a[@class="user-name"]/text()')
        user = tmp[0] if tmp else ''

        tmp = sel.xpath('//span[@class="useful-num"]/text()')
        try:
            vote_cnt = int(tmp[0])
        except (ValueError, IndexError):
            vote_cnt = 0

        paras = []
        for content in sel.xpath('//div[@class="c-content"]/p'):
            tmp = ''.join(content.itertext()).strip()
            if tmp:
                paras.append(tmp)
        contents = '\n\n'.join(paras)

        time_str = sel.xpath('//span[@class="time"]/text()')[0]
        ts = long((datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S') - timedelta(seconds=8 * 3600)
                   - datetime.utcfromtimestamp(0)).total_seconds() * 1000)

        data = {'authorName': user, 'authorAvatar': avatar, 'publishTime': ts, 'voteCnt': vote_cnt,
                'contents': contents}
        item_type = 'comment'
        yield item_type, data