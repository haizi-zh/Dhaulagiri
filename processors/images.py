# coding=utf-8
import argparse
import logging
import re
from hashlib import md5

import gevent
import pymongo
from pymongo.errors import DuplicateKeyError
import requests

from processors import BaseProcessor
from utils.database import get_mongodb


__author__ = 'zephyre'


class ImageUploader(BaseProcessor):
    """
    将图像从ImageCandidates中上传
    """

    name = 'image-upload'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)
        self.args = self.args_builder()

    def args_builder(self):
        parser = self.arg_parser
        parser.add_argument('--limit', type=int)
        parser.add_argument('--skip', default=0, type=int)
        parser.add_argument('--url-filter', type=str)
        parser.add_argument('--query', type=str)
        args, leftover = parser.parse_known_args()
        return args

    @staticmethod
    def check_exist(entry):
        """
        Check if an image is already processed
        """
        col_im = get_mongodb('imagestore', 'Images', 'mongo')

        url = entry['url']
        url_hash = md5(url).hexdigest()
        assert url_hash == entry['url_hash']
        ret = col_im.find_one({'url_hash': url_hash}, {'_id': 1})

        return bool(ret)

    @staticmethod
    def check_image(buf):
        """
        Check if an image is valid
        """
        pass

    @staticmethod
    def auth():
        """
        Authenticate

        :param key:
        :param bucket:
        """
        from qiniu import Auth
        from utils import load_yaml

        cfg = load_yaml()

        # 获得上传权限
        section = cfg['qiniu']
        ak = section['ak']
        sk = section['sk']
        q = Auth(ak, sk)

        return q

    def on_failure(self, entry):
        """
        Called on failure
        """
        col_cand = get_mongodb('imagestore', 'ImageCandidates', 'mongo')

        if 'failCnt' not in entry:
            entry['failCnt'] = 0
        entry['failCnt'] += 1
        self.logger.warn('Processing failed for image: %s' % entry['key'])
        col_cand.update({'_id': entry['_id']}, {'$set': {'failCnt': entry['failCnt']}})

    def upload_image(self, entry, response):
        """
        Upload the image to the qiniu bucket
        """
        from qiniu import put_data

        image = entry
        key = image['key']
        bucket = image['bucket']

        sc = False
        self.log('START UPLOADING: %s <= %s' % (key, response.url), logging.INFO)

        token = self.auth().upload_token(bucket, key)

        for idx in xrange(5):
            ret, info = put_data(token, key, response.content, check_crc=True)
            if not ret:
                self.log('UPLOADING FAILED #%d: %s, reason: %s' % (idx, key, info.error), logging.WARN)
                continue
            else:
                sc = True
                break
        if not sc:
            raise IOError
        self.log('UPLOADING COMPLETED: %s' % key, logging.INFO)

    def fetch_stat(self, entry):
        """
        Get stat for the image
        """
        from qiniu import BucketManager

        mgr = BucketManager(self.auth())

        ret, info = mgr.stat(entry['bucket'], entry['key'])

        if not ret:
            self.log('Failed to get stat for image: key=%s, bucket=%s' % (entry['key'], entry['bucket']), logging.WARN)
            raise IOError

        entry['size'] = ret['fsize']
        entry['hash'] = ret['hash']
        entry['type'] = ret['mimeType']
        entry['cTime'] = ret['putTime'] / 10000000

        return entry

    def fetch_info(self, entry):
        """
        Get image information
        """
        bucket = entry['bucket']
        key = entry['key']
        try:
            response = requests.get('http://%s.qiniudn.com/%s?imageInfo' % (bucket, key))
            image_info = response.json()

            if 'error' not in image_info:
                entry['cm'] = image_info['colorModel']
                entry['h'] = image_info['height']
                entry['w'] = image_info['width']
                entry['fmt'] = image_info['format']
                return entry
            else:
                raise IOError
        except IOError:
            self.log('Failed to get info for image: key=%s, bucket=%s' % (entry['key'], entry['bucket']), logging.WARN)
            raise IOError

    def proc_image(self, entry):
        """
        Process the imaeg item
        """
        col_cand = get_mongodb('imagestore', 'ImageCandidates', 'mongo')
        col_im = get_mongodb('imagestore', 'Images', 'mongo')

        entry['key'] = entry['url_hash']
        entry['bucket'] = 'aizou'

        if self.check_exist(entry):
            col_cand.remove({'_id': entry['_id']})
            return

        url = entry['url']
        try:
            response = requests.get(url)
            if response.status_code != 200:
                self.log('Failed to download download image (code=%d): key=%s, url=%s' % (
                    response.status_code, entry['key'], entry['url']),
                         logging.WARN)
                raise IOError

            self.upload_image(entry, response)
            self.fetch_stat(entry)
            self.fetch_info(entry)

            image_id = entry.pop('_id')
            if 'failCnt' in entry:
                entry.pop('failCnt')
            col_im.update({'url_hash': entry['url_hash']}, {'$set': entry}, upsert=True)
            col_cand.remove({'_id': image_id})

        except IOError:
            self.on_failure(entry)
            return

    def populate_tasks(self):
        col_cand = get_mongodb('imagestore', 'ImageCandidates', 'mongo')

        query = {'failCnt': {'$not': {'$gte': 5}}}

        extra_query = eval(self.args.query) if self.args.query else {}

        if extra_query:
            query = {'$and': [query, extra_query]}

        cursor = col_cand.find(query, snapshot=True)  # .sort('_id', pymongo.DESCENDING)
        if self.args.limit:
            cursor.limit(self.args.limit)
        cursor.skip(self.args.skip)

        for val in cursor:

            def task(entry=val):
                if self.args.url_filter:
                    pattern = self.args.url_filter
                    if not re.match(pattern, entry['url']):
                        self.log('Skipped image: %s' % entry['url'])
                        return

                self.log('Processing image: %s' % entry['url'])
                self.proc_image(entry)

            self.add_task(task)


class ImageTransfer(BaseProcessor):
    """
    图像的迁移。从lvxpingpai-img-store迁移到aizou，同时优化imagestore数据存储的格式
    """

    def populate_tasks(self):
        pass

    name = 'image-transfer'

    def __init__(self):
        super(ImageTransfer, self).__init__()
        self.args = self.args_builder()

    @staticmethod
    def args_builder():
        parser = argparse.ArgumentParser()
        parser.add_argument('cmd')
        parser.add_argument('--limit', default=None, type=int)
        parser.add_argument('--skip', default=0, type=int)
        args, leftover = parser.parse_known_args()
        return args

    @staticmethod
    def bucket_mgr():
        from qiniu import Auth
        from qiniu import BucketManager
        from utils import load_yaml

        conf = load_yaml()['qiniu']

        access_key = conf['ak']
        secret_key = conf['sk']
        q = Auth(access_key, secret_key)
        return BucketManager(q)

    @staticmethod
    def mv_candidates(image):
        """
        将图像添加到ImageCandidates里面
        """
        from utils.database import get_mongodb

        col_cand = get_mongodb('imagestore', 'ImageCandidates', profile='mongo')
        col_img = get_mongodb('imagestore', 'Images', profile='mongo')

        image_id = image.pop('_id')

        print 'Moving %s' % image['url']
        image['url_hash'] = md5(image['url']).hexdigest()
        image['key'] = image['url_hash']
        col_cand.update({'url_hash': image['url_hash']}, {'$set': image})

        col_img.remove({'_id': image_id})

    def run(self):
        from utils.database import get_mongodb

        col = get_mongodb('imagestore', 'Images', profile='mongo')

        cursor = col.find({}).sort('_id', pymongo.ASCENDING)
        cursor.skip(self.args.skip)
        if self.args.limit:
            cursor.limit(self.args.limit)

        bmgr = self.bucket_mgr()

        # self.total = cursor.count(with_limit_and_skip=True)
        self.total = 0

        super(ImageTransfer, self).run()

        for entry in cursor:
            def func(val=entry):
                self.progress += 1

                print 'Processing %s : %s' % (val['url_hash'], val['url'])
                url_hash = md5(val['url']).hexdigest()
                key = re.search(r'[0-9a-f]{32}', val['key']).group()
                if url_hash != val['url_hash'] or key != url_hash:
                    self.mv_candidates(val)
                    return

                bucket_name = val['bucket'] if 'bucket' in val else 'lvxingpai-img-store'
                key = val['key']

                bmgr.copy(bucket_name, key, 'aizou', val['url_hash'])

                ret, info = bmgr.stat('aizou', val['url_hash'])
                if ret is None and info.status_code == 612:
                    # 如果爱走里面也找不到，则添加到ImageCandidates库里面，等待后续重新下载
                    self.mv_candidates(val)
                    return

                assert ret is not None
                val['cTime'] = long(ret['putTime']) / 10000
                val['type'] = ret['mimeType']
                val['hash'] = ret['hash']
                val['size'] = ret['fsize']
                val['key'] = url_hash

                if '_id' in val:
                    val.pop('_id')

                print 'Updating %s: %s' % (val['url_hash'], val['url'])
                try:
                    col.update({'url_hash': val['url_hash']}, {'$set': val}, upsert=True)
                except DuplicateKeyError:
                    ret = col.find_one({'key': val['key']})
                    self.mv_candidates(ret)
                    col.update({'url_hash': val['url_hash']}, {'$set': val}, upsert=True)

            self.add_task(func)
            gevent.sleep(0)

        self._join()


class ImageValidator(BaseProcessor):
    """
    图像验证
    """

    def populate_tasks(self):
        pass

    name = 'image-validate'

    def __init__(self):
        super(ImageValidator, self).__init__()
        self.args = self.args_builder()

    @staticmethod
    def args_builder():
        parser = argparse.ArgumentParser()
        parser.add_argument('cmd')
        parser.add_argument('--limit', default=None, type=int)
        parser.add_argument('--skip', default=0, type=int)
        parser.add_argument('--db', required=True)
        parser.add_argument('--col', required=True)
        args, leftover = parser.parse_known_args()
        return args

    def run(self):
        from utils.database import get_mongodb

        col = get_mongodb(self.args.db, self.args.col, profile='mongo')

        cursor = col.find({'images': {'$ne': None}}, snapshot=True)
        cursor.skip(self.args.skip)
        if self.args.limit:
            cursor.limit(self.args.limit)

        col_im = get_mongodb('imagestore', 'Images', profile='mongo')

        cursor = col.find({'images': {'$ne': None}}, {'images': 1}).sort('_id', pymongo.ASCENDING)
        cursor.skip(self.args.skip)
        if self.args.limit:
            cursor.limit(self.args.limit)

        self.total = 0
        super(ImageValidator, self).run()
        for entry in cursor:
            def func(val=entry):
                modified = False
                if 'images' not in val or not val['images']:
                    return

                for img in val['images']:
                    key = img['key']
                    match = re.search(r'[0-9a-f]{32}', key)
                    if not match:
                        continue
                    new_key = match.group()

                    # 使用new_key去imagestore中查询
                    ret = col_im.find_one({'$or': [{'key': new_key}, {'url_hash': new_key}]})
                    if not ret:
                        print 'Image not exists: %s' % key
                        continue

                    if img['key'] != new_key:
                        modified = True
                        img['key'] = new_key

                    if 'url' in img:
                        modified = True
                        img.pop('url')

                    if 'cropHint' in img:
                        ch = img['cropHint']
                        if ch['bottom'] == 0 and ch['right'] == 0:
                            modified = True
                            img.pop('cropHint')

                if modified:
                    print 'Updating %s' % val['_id']
                    col.update({'_id': val['_id']}, {'$set': {'images': val['images']}})

            self.add_task(func)
            gevent.sleep(0)

        self._join()


