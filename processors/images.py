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
            eid = entry['_id']

            print eid
            ret, info = bmgr.copy(bucket_name, key, 'aizou', entry['url_hash'])

            if ret != {} and info.error == 'file exists':
                pass
            else:
                assert ret == {}

            print eid
            ret, info = bmgr.stat('aizou', entry['url_hash'])
            assert ret is not None
            entry['cTime'] = long(ret['putTime']) / 10000
            entry['type'] = ret['mimeType']
            entry['hash'] = ret['hash']
            entry['size'] = ret['fsize']
            entry['key'] = entry['url_hash']

            print eid
            col.save(entry)

        for entry in cursor:
            jobs.append(gevent.spawn(gl, entry))

        gevent.joinall(jobs)

