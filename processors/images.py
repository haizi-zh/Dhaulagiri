# coding=utf-8
import argparse
import re
from hashlib import md5
import gevent
import pymongo
from pymongo.errors import DuplicateKeyError
from processors import BaseProcessor

__author__ = 'zephyre'


class ImageTransfer(BaseProcessor):
    """
    图像的迁移。从lvxpingpai-img-store迁移到aizou，同时优化imagestore数据存储的格式
    """

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
        from mongo import load_config

        conf = load_config()['qiniu']

        access_key = conf['ak']
        secret_key = conf['sk']
        q = Auth(access_key, secret_key)
        return BucketManager(q)

    @staticmethod
    def mv_candidates(image):
        """
        将图像添加到ImageCandidates里面
        """
        from mongo import get_mongodb

        col_cand = get_mongodb('imagestore', 'ImageCandidates', profile='mongo')
        col_img = get_mongodb('imagestore', 'Images', profile='mongo')

        image_id = image.pop('_id')

        print 'Moving %s' % image['url']
        image['url_hash'] = md5(image['url']).hexdigest()
        image['key'] = image['url_hash']
        col_cand.update({'url_hash': image['url_hash']}, {'$set': image})

        col_img.remove({'_id': image_id})

    def run(self):
        from mongo import get_mongodb

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

        self.join()


class ImageValidator(BaseProcessor):
    """
    图像验证
    """

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
        from mongo import get_mongodb

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
                    print 'Updateing %s' % val['_id']
                    col.update({'_id': val['_id']}, {'$set': {'images': val['images']}})

            self.add_task(func)
            gevent.sleep(0)

        self.join()


