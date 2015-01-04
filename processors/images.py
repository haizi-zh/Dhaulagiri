# coding=utf-8
import argparse
import re

__author__ = 'zephyre'


class ImageTransfer(object):
    """
    图像的迁移。从lvxpingpai-img-store迁移到aizou，同时优化imagestore数据存储的格式
    """

    name = 'image-transfer'

    def __init__(self):
        self.args = None

        self.args_builder()

    def args_builder(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('cmd')
        parser.add_argument('--limit', default=None, type=int)
        parser.add_argument('--skip', default=0, type=int)
        self.args = parser.parse_args()

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

    def run(self):
        import gevent
        from mongo import get_mongodb

        col = get_mongodb('imagestore', 'Images', profile='mongo')

        cursor = col.find({'key': re.compile(r'^assets'), 'bucket': None}, snapshot=True)
        cursor.skip(self.args.skip)
        if self.args.limit:
            cursor.limit(self.args.limit)

        bmgr = self.bucket_mgr()
        jobs = []

        def gl(entry):
            bucket_name = entry['bucket'] if 'bucket' in entry else 'lvxingpai-img-store'
            key = entry['key']

            ret, info = bmgr.copy(bucket_name, key, 'aizou', entry['url_hash'])

            if ret != {} and info.error == 'file exists':
                pass
            else:
                assert ret == {}

            ret, info = bmgr.stat('aizou', entry['url_hash'])
            assert ret is not None
            entry['cTime'] = long(ret['putTime']) / 10000
            entry['type'] = ret['mimeType']
            entry['hash'] = ret['hash']
            entry['size'] = ret['fsize']
            entry['key'] = entry['url_hash']

            col.save(entry)

        print '%d documents to process...' % cursor.count(with_limit_and_skip=True)
        for entry in cursor:
            jobs.append(gevent.spawn(gl, entry))

        gevent.joinall(jobs)


class ImageValidator(object):
    """
    图像验证
    """

    name = 'image-validate'

    def __init__(self):
        self.args = None

        self.args_builder()

    def args_builder(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('cmd')
        parser.add_argument('--limit', default=None, type=int)
        parser.add_argument('--skip', default=0, type=int)
        parser.add_argument('--db', required=True)
        parser.add_argument('--col', required=True)
        self.args = parser.parse_args()

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

    def run(self):
        import gevent
        from mongo import get_mongodb

        col = get_mongodb(self.args.db, self.args.col, profile='mongo')

        cursor = col.find({'images': {'$ne': None}}, snapshot=True)
        cursor.skip(self.args.skip)
        if self.args.limit:
            cursor.limit(self.args.limit)

        col_im = get_mongodb('imagestore', 'Images', profile='mongo')

        def func(entry):
            modified = False
            for img in entry['images']:
                key = img['key']
                match = re.search(r'assets/images/([0-9a-f]{32})', key)
                if match:
                    key = match.group(1)
                ret = col_im.find_one({'key': key})
                if not ret:
                    print img
                assert ret is not None

                if img['key'] != key:
                    modified = True
                    img['key'] = key

                if 'url' in img:
                    modified = True
                    img.pop('url')

                if 'cropHint' in img:
                    ch = img['cropHint']
                    if ch['bottom'] == 0 and ch['right'] == 0:
                        modified = True
                        img.pop('cropHint')

            if modified:
                col.save(entry)

        jobs = []
        for entry in cursor:
            jobs.append(gevent.spawn(func, entry))

        gevent.joinall(jobs)


