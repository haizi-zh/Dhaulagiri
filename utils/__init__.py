__author__ = 'zephyre'


def load_config():
    """
    Load configuration files from ./conf/*.cfg
    """

    conf = getattr(load_config, 'conf', {})

    if conf:
        return conf
    else:
        import ConfigParser
        import os

        root_dir = os.path.normpath(os.path.split(__file__)[0])
        cfg_dir = os.path.normpath(os.path.join(root_dir, '../conf'))
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