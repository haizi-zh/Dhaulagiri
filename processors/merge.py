from processors import BaseProcessor
from utils import load_yaml
from utils.database import get_mongodb

__author__ = 'zephyre'


class BaseMerger(object):
    def __init__(self):
        self.fields = set([])
        self.priority = 1

    def process(self, source, target):
        raise NotImplementedError

    def add_fields(self, fields):
        for f in fields:
            self.fields.add(f)


class Appender(BaseMerger):
    def process(self, source, target):
        for f in self.fields:
            if f not in source or (f in target and target[f]):
                continue
            target[f] = source[f]


class Overwriter(BaseMerger):
    def process(self, source, target):
        for f in self.fields:
            if f not in source or not source[f]:
                continue
            target[f] = source[f]


class SetAdder(BaseMerger):
    def process(self, source, target):
        import collections

        for f in self.fields:
            if f not in source or (f in target and target[f] and not isinstance(target[f], collections.Iterable)):
                continue

            if f not in target or not target[f]:
                data = set([])
            else:
                data = set(target[f])

            for item in source[f]:
                data.add(item)

            if data:
                target[f] = list(data)


class BaiduMergeProcessor(BaseProcessor):
    name = 'baidu-merger'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)

        self.args = self.args_builder()

    def args_builder(self):
        parser = self.arg_parser
        parser.add_argument('--limit', default=None, type=int)
        parser.add_argument('--skip', default=0, type=int)
        parser.add_argument('--merger', nargs='*', required=True)
        parser.add_argument('--type', choices=['mdd', 'vs'], required=True)
        return parser.parse_args()

    def populate_tasks(self):
        col_src, db_tar, col_tar = ('BaiduLocality', 'geo', 'Locality') if self.args.type == 'mdd' else (
            'BaiduPoi', 'poi', 'ViewSpot')
        col = get_mongodb('proc_baidu', col_src, profile='mongo-raw')

        col_target = get_mongodb(db_tar, col_tar, profile='mongo')

        cursor = col.find({})
        cursor.skip(self.args.skip)
        if self.args.limit:
            cursor.limit(self.args.limit)

        mergers = []
        merger_rules = load_yaml()['merger']

        for name in self.args.merger:
            rule = merger_rules[name]
            m = globals()[rule['class']]()

            m.add_fields(rule['fields'])
            if 'priority' in rule:
                m.priority = rule['priority']
            mergers.append(m)
        mergers = sorted(mergers, key=lambda v: v.priority)

        for val in cursor:
            def func(entry=val):
                self.log(u'Processing: zhName=%s, sid=%s, surl=%s' % (entry['zhName'], entry['source']['baidu']['id'],
                                                                      entry['source']['baidu']['surl']))
                target = col_target.find_one({'isEdited': {'$ne': True},
                                              'source.baidu.id': entry['source']['baidu']['id']})
                if not target and 'mafengwo' in entry['source']:
                    target = col_target.find_one({'isEdited': {'$ne': True},
                                                  'source.mafengwo.id': entry['source']['mafengwo']['id']})
                if not target:
                    return

                self.log(u'Merging: zhName=%s, sid=%s, surl=%s' % (entry['zhName'], entry['source']['baidu']['id'],
                                                                   entry['source']['baidu']['surl']))
                for m in mergers:
                    m.process(entry, target)

                target['taoziEna'] = True

                col_target.save(target)

            self.add_task(func)



