# coding=utf-8
import pysolr
import threading
from utils import load_yaml

__author__ = 'zephyre'


def static_var(var, value):
    """
    Decorator to support static variables in functions.
    """
    def decorate(func):
        setattr(func, var, value)
        return func

    return decorate


@static_var('conf', None)
def load_mongodb_conf():
    """
    Load MongoDB configurations
    :return:
    """
    cached_conf = load_mongodb_conf.conf

    if not cached_conf:
        conf_all = load_yaml()
        tmp = conf_all['mongodb'] if 'mongodb' in conf_all else []
        cached_conf = dict((item['profile'], item) for item in tmp)
        load_mongodb_conf.conf = cached_conf

    return cached_conf


def init_mongodb_client(conf_item):
    section = conf_item
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

    setattr(client, 'auth_list', set([]))

    return client


def auth_mongodb_client(client, auth_info):
    """
    MongoDB authentication

    :param auth_info: { 'credb': '', 'user': '', 'passwd': '' }
    """
    auth_sig = '%s|%s' % (auth_info['credb'], auth_info['user'])
    auth_list = client.auth_list

    if auth_sig not in auth_list:
        db_auth = auth_info['credb']
        user = auth_info['user']
        passwd = auth_info['passwd']
        client[db_auth].authenticate(name=user, password=passwd)
        auth_list.add(auth_sig)


@static_var('cached_clients', {})
@static_var('lock', threading.Lock())
def get_mongodb(db_name, col_name, profile):
    """
    Establish a MongoDB connection
    """
    cached_clients = get_mongodb.cached_clients
    client = cached_clients[profile] if profile in cached_clients else None

    mongodb_conf = load_mongodb_conf()[profile]

    if not client:
        lock = get_mongodb.lock
        lock.acquire()
        try:
            client = cached_clients[profile] if profile in cached_clients else None
            if not client:
                client = init_mongodb_client(mongodb_conf)
                cached_clients[profile] = client
        finally:
            lock.release()

    if 'auth' in mongodb_conf and mongodb_conf['auth']:
        auth_mongodb_client(client, mongodb_conf['auth'])

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
