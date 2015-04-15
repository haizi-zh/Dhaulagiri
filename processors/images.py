# coding=utf-8
from bson.json_util import dumps, loads
import requests

from app import *
from utils import get_logger
from utils.database import get_mongodb


__author__ = 'zephyre'

coll_cand = get_mongodb('imagestore', 'ImageCandidates', 'mongo')
coll_im = get_mongodb('imagestore', 'Images', 'mongo')
logger = get_logger()


@app.task(serializer='json', name='processors.images.upload')
def image_upload_task(data):
    entry = loads(data)
    entry['key'] = entry['url_hash']
    entry['bucket'] = 'aizou'

    key = entry['url_hash']
    if not _check_existence(key):
        logger.info('Fetching %s' % key)
        _fetch(entry)
        _fetch_stat(entry)
        if not entry['type'].startswith('image/'):
            raise IOError
        _fetch_info(entry)

        image_id = entry.pop('_id')
        if 'failCnt' in entry:
            entry.pop('failCnt')

        logger.info(entry)
        coll_im.update({'url_hash': entry['url_hash']}, {'$set': entry}, upsert=True)
        # coll_cand.remove({'_id': image_id})
    else:
        logger.info('%s already exists, skipping...' % key)


def _fetch_stat(entry):
    """
    Get stat for the image
    """
    from qiniu import BucketManager

    mgr = BucketManager(_qiniu_auth())
    ret, info = mgr.stat(entry['bucket'], entry['key'])
    if not ret:
        logger.error('Failed to get stat for image: key=%s, bucket=%s' % (entry['key'], entry['bucket']))
        raise IOError

    entry['size'] = ret['fsize']
    entry['hash'] = ret['hash']
    entry['type'] = ret['mimeType']
    entry['cTime'] = ret['putTime'] / 10000000

    return entry


def _fetch_info(entry):
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
        logger.error('Failed to get info for image: key=%s, bucket=%s' % (entry['key'], entry['bucket']))
        raise IOError


def _qiniu_auth():
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


def _add_fail_cnt(key):
    coll_cand.update({'url_hash': key}, {'$inc': {'failCnt': 1}})


def _fetch(entry):
    """
    使用七牛获取图像
    """
    key = entry['key']
    bucket = entry['bucket']
    url = entry['url']

    from qiniu import BucketManager

    bucket_mgr = BucketManager(_qiniu_auth())
    fetch_result = bucket_mgr.fetch(url, bucket, key)
    status_code = None
    try:
        status_code = fetch_result[1].status_code
        if fetch_result[1].exception is not None or status_code != 200:
            raise IOError
    except (IndexError, IOError, AttributeError) as e:
        _add_fail_cnt(key)
        logger.error('Error fetching image: %s, status: %d' % (url, status_code))
        raise e

    return entry


def _check_existence(key):
    """
    检查某图像在Images集合中是否存在
    :param key:
    :return:
    """
    ret = coll_im.find_one({'key': key}, {'_id': 1})
    return bool(ret)


def image_upload():
    cursor = coll_cand.find({'failCnt': None})

    for entry in cursor:
        image_upload_task.delay(dumps(entry))