# coding=utf-8
__author__ = 'zephyre'


def load_config():
    """
    加载config文件
    """

    conf = getattr(load_config, 'conf', {})

    if conf:
        return conf
    else:
        import ConfigParser
        import os

        root_dir = os.path.normpath(os.path.split(__file__)[0])
        cfg_dir = os.path.normpath(os.path.join(root_dir, 'conf'))
        it = os.walk(cfg_dir)
        cf = ConfigParser.ConfigParser()
        for f in it.next()[2]:
            if os.path.splitext(f)[-1] != '.cfg':
                continue
            cf.read(os.path.normpath(os.path.join(cfg_dir, f)))

            for s in cf.sections():
                section = {}
                for opt in cf.options(s):
                    section[opt] = cf.get(s, opt)
                conf[s] = section

        setattr(load_config, 'conf', conf)
        return conf


def get_mongodb(db_name, col_name, profile=None, host='localhost', port=27017, user=None, passwd=None):
    """
    建立MongoDB的连接。
    :param host:
    :param port:
    :param db_name:
    :param col_name:
    :return:
    """
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
    return db[col_name]