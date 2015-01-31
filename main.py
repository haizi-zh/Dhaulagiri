# coding=utf-8
import types

import conf
from utils import load_yaml


__author__ = 'zephyre'

from gevent import monkey

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
                        target_cls = getattr(mod, attr_name)
                        name = getattr(target_cls, 'name')
                        func = getattr(target_cls, 'run')
                        if isinstance(name, str) and isinstance(func, types.MethodType):
                            conf.global_conf['processors'][name] = target_cls
                        else:
                            continue
                    except (TypeError, AttributeError):
                        pass
            except ImportError:
                print 'Import error: %s' % fname
                raise


def test():
    parser = argparse.ArgumentParser()
    parser.add_argument('cmd')
    args, leftovers = parser.parse_known_args()

    load_yaml()

    reg_processors()

    if args.cmd in conf.global_conf['processors']:
        parser_cls = conf.global_conf['processors'][args.cmd]
        proc = parser_cls(arg_parser=parser)
        proc.run()
    else:
        print 'No processor found for: %s' % args.cmd


def main():
    from core import ProcessorEngine

    parser = argparse.ArgumentParser()
    parser.add_argument('cmd', type=str)
    args, leftovers = parser.parse_known_args()

    engine = ProcessorEngine.get_instance()

    engine.add_processor(args.cmd)

    engine.start()


if __name__ == '__main__':
    main()