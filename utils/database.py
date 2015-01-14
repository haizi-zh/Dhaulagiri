# coding=utf-8
import pysolr
from utils import load_config
from utils import load_yaml

__author__ = 'zephyre'


def get_mongodb(db_name, col_name, profile):
    """
    建立MongoDB的连接。
    :param host:
    :param port:
    :param db_name:
    :param col_name:
    :return:
    """

    cached = getattr(get_mongodb, 'cached', {})

    if profile in cached:
        return cached[profile][db_name][col_name]

    cfg = load_yaml()
    section = filter(lambda v: v['profile'] == profile, cfg['mongodb'])[0]

    host = section.get('host', 'localhost')
    port = int(section.get('port', '27017'))

    if section.get('replica', False):
        from pymongo import MongoReplicaSetClient
        from pymongo import ReadPreference

        client = MongoReplicaSetClient('%s:%d' % (host, port), replicaSet=section['replName'])

        pref = section.get('readPref', 'PRIMARY')
        client.read_preference = getattr(ReadPreference, pref)

    else:
        from pymongo import MongoClient

        client = MongoClient(host, port)

    cached[profile] = client
    setattr(get_mongodb, 'cached', cached)

    db = client[db_name]

    if section.get('auth', False):
        user = section.get('user', None)
        passwd = section.get('passwd', None)
        db.authenticate(name=user, password=passwd)

    return db[col_name]


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

    cfg = load_yaml()
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
    """
    Get solr connection.

    :param profile:
    :return:
    """
    cached = getattr(get_solr, 'cached', {})

    if profile in cached:
        return cached[profile]

    cfg = load_yaml()
    section = filter(lambda v: v['profile'] == profile, cfg['solr'])[0]

    host = section.get('host', 'localhost')
    port = int(section.get('port', '27017'))
    collection = section['collection']
    client = pysolr.Solr('http://%s:%d/solr/%s' % (host, port, collection))

    cached[profile] = client
    setattr(get_mongodb, 'cached', cached)

    return client