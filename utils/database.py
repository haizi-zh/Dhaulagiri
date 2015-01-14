# coding=utf-8
import pysolr
from utils import load_config

__author__ = 'zephyre'


def get_mongodb(db_name, col_name, profile=None, host='localhost', port=27017, user=None, passwd=None):
    """
    建立MongoDB的连接。
    :param host:
    :param port:
    :param db_name:
    :param col_name:
    :return:
    """

    cached = getattr(get_mongodb, 'cached', {})
    sig = '%s|%s|%s|%s|%s|%s|%s' % (db_name, col_name, profile, host, port, user, passwd)
    if sig in cached:
        return cached[sig]

    cfg = dict(load_config())
    if profile and profile in cfg:
        section = cfg[profile]
        host = section.get('host', 'localhost')
        port = int(section.get('port', '27017'))
        user = section.get('user', None)
        passwd = section.get('passwd', None)

    from pymongo import MongoClient

    mongo_conn = MongoClient(host, port)
    db = mongo_conn[db_name]
    if user and passwd:
        db.authenticate(name=user, password=passwd)
    col = db[col_name]

    cached[sig] = col
    setattr(get_mongodb, 'cached', cached)
    return col


def get_mysql_db(db_name, user=None, passwd=None, profile=None, host='localhost', port=3306):
    """
    建立MySQL连接
    :param db_name:
    :param user:
    :param passwd:
    :param profile:
    :param host:
    :param port:
    :return:
    """

    cached = getattr(get_mysql_db, 'cached', {})
    sig = '%s|%s|%s|%s|%s|%s' % (db_name, profile, host, port, user, passwd)
    if sig in cached:
        return cached[sig]

    cfg = dict(load_config())
    if profile and profile in cfg:
        section = cfg[profile]
        host = section.get('host', 'localhost')
        port = int(section.get('port', '3306'))
        user = section.get('user', None)
        passwd = section.get('passwd', None)

    from MySQLdb.cursors import DictCursor
    import MySQLdb

    return MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db_name, cursorclass=DictCursor,
                           charset='utf8')


def get_solr(profile):
    cfg = dict(load_config())
    section = cfg[profile]
    host = section.get('host')
    port = section.get('port')
    # solr配置
    solr_s = pysolr.Solr('http://%s:%s/solr/travelnote' % (host, port))

    return solr_s