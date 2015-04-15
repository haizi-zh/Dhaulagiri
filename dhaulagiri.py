# coding=utf-8
from utils import get_logger

__author__ = 'zephyre'

exec 'from app import *'

logger = get_logger()


def register():
    """
    将processors路径下的processor类进行注册
    """
    print 'Registering processors...'
    logger.info('Registering processors...')

    import os
    import imp

    root_dir = os.path.normpath(os.path.split(__file__)[0])
    proc_dir = os.path.normpath(os.path.join(root_dir, 'processors'))

    for cur, d_list, f_list in os.walk(proc_dir):
        for f in f_list:
            f = os.path.normpath(os.path.join(cur, f))
            tmp, ext = os.path.splitext(f)
            if ext != '.py':
                continue
            p, fname = os.path.split(tmp)

            try:
                ret = imp.find_module(fname, [p]) if p else imp.find_module(fname)
                imp.load_module(fname, *ret)
            except ImportError:
                print 'Import error: %s' % fname
                raise


def worker():
    register()

    from app import app
    import sys
    del sys.argv[1]

    logger.info('Starting the worker...')
    app.worker_main()


def master():
    from processors.images import image_upload

    image_upload()


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('cmd', choices=['master', 'worker'])
    args = parser.parse_args()
    if args.cmd == 'master':
        master()
    else:
        worker()


if __name__ == '__main__':
    main()
else:
    register()