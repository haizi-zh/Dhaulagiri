# coding=utf-8
import conf

__author__ = 'zephyre'

from gevent import monkey;

monkey.patch_all()
import argparse


def reg_processors(proc_dir=None):
    """
    将processors路径下的processor类进行注册
    """
    import os
    import imp

    if not proc_dir:
        root_dir = os.path.normpath(os.path.split(__file__)[0])
        proc_dir = os.path.normpath(os.path.join(root_dir, 'processors'))

    conf.global_conf['processors'] = {}

    for cur, d_list, f_list in os.walk(proc_dir):
        for f in f_list:
            f = os.path.normpath(os.path.join(cur, f))
            tmp, ext = os.path.splitext(f)
            if ext != '.py':
                continue
            p, fname = os.path.split(tmp)

            try:
                ret = imp.find_module(fname, [p]) if p else imp.find_module(fname)
                mod = imp.load_module(fname, *ret)

                for attr_name in dir(mod):
                    try:
                        c = getattr(mod, attr_name)
                        if issubclass(c, object):
                            name = getattr(c, 'name')
                            if name:
                                conf.global_conf['processors'][name] = c
                    except (TypeError, AttributeError):
                        pass
            except ImportError:
                print 'Import error: %s' % fname
                raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('cmd')
    parser.add_argument('args', nargs=argparse.REMAINDER)
    args = parser.parse_args()

    reg_processors()

    if args.cmd in conf.global_conf['processors']:
        proc = conf.global_conf['processors'][args.cmd]()
        proc.run()


if __name__ == '__main__':
    main()
