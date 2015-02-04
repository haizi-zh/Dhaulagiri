# coding=utf-8
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

    cached = getattr(get_mongodb, 'cached', None)
    if cached is None:
        cached = {}
        setattr(get_mongodb, 'cached', cached)

    client = cached[profile] if profile in cached else None

    if not client:
        cfg = load_yaml()
        section = filter(lambda v: v['profile'] == profile, cfg['mongodb'])[0]
        servers = section.get('servers')

        def build_node_desc(node_conf):
            return '%s:%s' % (node_conf['host'], node_conf['port'])

        if section.get('replica', False):
            from pymongo import MongoReplicaSetClient
            from pymongo import ReadPreference

            client = MongoReplicaSetClient(','.join(map(build_node_desc, servers)), replicaSet=section.get('replName'))
            pref = section.get('readPref', 'PRIMARY')
            client.read_preference = getattr(ReadPreference, pref)
        else:
            from pymongo import MongoClient

            s = servers[0]
            client = MongoClient(s['host'], s['port'])

        auth = section.get('auth')
        if auth:
            db_auth = auth['credb']
            user = auth['user']
            passwd = auth['passwd']
            client[db_auth].authenticate(name=user, password=passwd)

        cached[profile] = client

    return client[db_name][col_name]


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