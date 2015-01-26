# coding=utf-8
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


class ImageMerger(BaseMerger):
    def process(self, source, target):
        if 'isDone' in target and target['isDone']:
            return

        if 'images' in source and source['images']:
            target['images'] = source['images']


class ExceptFieldsMerger(BaseMerger):
    """
    除了某些fields以外，剩下的一律覆盖
    """

    def process(self, source, target):
        for k, v in source.items():
            # 除了几种特殊情况以外，一律覆盖
            if k == 'images' and 'isDone' in target and target['isDone']:
                continue
            elif k in self.fields and 'isEdited' in target and target['isEdited'] and k in target and target[k]:
                continue
            if k == '_id':
                continue

            target[k] = source[k]


class EditorMerger(BaseMerger):
    """
    处理一些可能已经被编辑修改的字段
    """

    def process(self, source, target):
        for f in self.fields:
            if f not in source or not source[f]:
                continue

            if 'isEdited' in target and target['isEdited'] and f in target and target[f]:
                continue

            target[f] = source[f]


class BaiduMergeProcessor(BaseProcessor):
    name = 'baidu-merger'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)

        self.args = self.args_builder()

        # 初始化merger
        self.mergers = []
        merger_rules = load_yaml()['merger']

        name_list = self.args.merger if self.args.merger else [
            'LocalityAppender' if self.args.type == 'mdd' else 'PoiAppender', 'SetAdder', 'Overwriter', 'ImageMerger',
            'EditorMerger']

        for name in name_list:
            rule = merger_rules[name]
            m = globals()[rule['class']]()

            if 'fields' in rule and rule['fields']:
                m.add_fields(rule['fields'])

            if 'priority' in rule:
                m.priority = rule['priority']

            self.mergers.append(m)
        self.mergers = sorted(self.mergers, key=lambda v: v.priority)

    def args_builder(self):
        parser = self.arg_parser
        parser.add_argument('--limit', default=None, type=int)
        parser.add_argument('--skip', default=0, type=int)
        parser.add_argument('--merger', nargs='*')
        parser.add_argument('--type', choices=['mdd', 'vs'], required=True)
        args, leftover = parser.parse_known_args()
        return args

    @staticmethod
    def resolve_targets(data):
        """
        将baidu sid解析为相应的object ID
        :param entry:
        :param data:
        :return:
        """
        if 'locList' not in data:
            return

        col_country = get_mongodb('geo', 'Country', 'mongo')
        col_mdd = get_mongodb('geo', 'Locality', 'mongo')

        def func(loc_list):
            """
            顺序查找loc_list中的项目。如果有命中的，则返回。
            :param col:
            :param loc_list:
            :return:
            """
            target_list = []
            country = None
            country_flag = True

            for item in loc_list:
                if country_flag:
                    ret = col_country.find_one({'alias': item['sname']}, {'zhName': 1, 'enName': 1})
                else:
                    ret = col_mdd.find_one({'source.baidu.id': item['sid']}, {'zhName': 1, 'enName': 1})
                if not ret:
                    continue

                if country_flag:
                    country = ret
                    country_flag = False

                target_list.append(ret)

            return country, target_list

        country, target_list = func(data.pop('locList'))
        if country:
            data['country'] = country
            data['abroad'] = country['zhName'] not in [u'中国', u'澳门', u'香港', u'台湾']
        else:
            data['abroad'] = None
        if target_list:
            data['locList'] = target_list

    def populate_tasks(self):
        col_src, db_tar, col_tar = ('BaiduLocality', 'geo', 'LocalityTransfer') if self.args.type == 'mdd' else (
            'BaiduPoi', 'poi', 'ViewSpotTransfer')
        col = get_mongodb('proc_baidu', col_src, profile='mongo-raw')

        col_target = get_mongodb(db_tar, col_tar, profile='mongo')

        cursor = col.find({})
        cursor.skip(self.args.skip)
        if self.args.limit:
            cursor.limit(self.args.limit)

        for val in cursor:
            def func(entry=val):
                surl = entry['source']['baidu']['surl'] if 'surl' in entry['source']['baidu'] else ''
                self.log(u'Processing: zhName=%s, sid=%s, surl=%s' % (entry['zhName'], entry['source']['baidu']['id'],
                                                                      surl))
                self.resolve_targets(entry)
                target = col_target.find_one({'source.baidu.id': entry['source']['baidu']['id']})
                if not target:
                    target = {}

                for m in self.mergers:
                    m.process(entry, target)

                if target:
                    target['taoziEna'] = True
                    target['lxpEna'] = True
                    col_target.save(target)

            self.add_task(func)



