# coding=utf-8
import conf

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


def load_processor(cls, args, arg_parser):
    """
    初始化一个processor

    :param cls: processor的类
    :param args: ArgumentParser生成的命令行参数
    :return:
    """
    processor = cls(arg_parser=arg_parser)

    import os
    import logging
    from logging.handlers import TimedRotatingFileHandler
    from logging import StreamHandler, Formatter

    # Set up a specific logger with our desired output level
    logger = logging.getLogger(processor.processor_name)

    if args.verbose:
        handler = StreamHandler()
    else:
        if args.logpath:
            log_path = os.path.abspath(args.logpath)
        else:
            log_path = os.path.abspath(os.path.join(os.path.split(__file__)[0], 'log'))
        log_file = os.path.normpath(os.path.join(log_path, '%s.log' % processor.name))
        handler = TimedRotatingFileHandler(log_file, when='d', encoding='utf-8')

    log_level = logging.DEBUG if args.debug else logging.INFO
    handler.setLevel(log_level)

    formatter = Formatter(fmt='%(asctime)s [%(name)s] %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S%z')
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.setLevel(log_level)

    return processor


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('cmd')
    parser.add_argument('--debug', action='store_true', default=False)
    parser.add_argument('--verbose', action='store_true', default=False)
    parser.add_argument('--logpath', type=str)
    args, leftovers = parser.parse_known_args()

    reg_processors()

    if args.cmd in conf.global_conf['processors']:
        proc = load_processor(conf.global_conf['processors'][args.cmd], args, parser)
        proc.run()


if __name__ == '__main__':
    main()