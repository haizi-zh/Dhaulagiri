# coding=utf-8
import json
import logging
import re

from processors import BaseProcessor
from utils.database import get_mongodb
from utils.mixin import MfwSuggestion, BaiduSuggestion


__author__ = 'zephyre'


class BaiduSceneProcessor(BaseProcessor, MfwSuggestion, BaiduSuggestion):
    name = 'baidu-scene'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)

        self.args = self.args_builder()

    def args_builder(self):
        parser = self.arg_parser
        parser.add_argument('--limit', default=None, type=int)
        parser.add_argument('--skip', default=0, type=int)
        parser.add_argument('--type', choices=['mdd', 'vs'], required=True)
        parser.add_argument('--mfw-match', default=False, action='store_true')
        parser.add_argument('--query', type=str)
        args, leftover = parser.parse_known_args()
        return args

    def get_type_code(self, entry):
        """
        Get type code for a certain entry, which indicates a destination while type_code<=5,
        and a view spot while type_code==6

        :param entry:
        :return:
        """
        for name in [entry[key] for key in ['sname', 'ambiguity_sname', 'surl']]:
            baidu_entry = filter(lambda v: v['surl'] == entry['surl'], self.get_baidu_sug(name, None))
            if baidu_entry:
                entry['type_code'] = baidu_entry[0]['type_code']
                return entry

    def match_mfw(self, entry, location):
        scene_type = 'mdd' if entry['is_locality'] else 'vs'

        name_list = []
        for key in ['sname', 'ambiguity_sname']:
            name = entry[key]
            if name not in name_list:
                name_list.append(name)

        for name in name_list:
            mfw_sug = self.get_mfw_sug(name, scene_type, location)

            if mfw_sug:
                self.log(u'Matched: %s(%s) <= %s(id=%d)' % (entry['sname'], entry['surl'], mfw_sug[0]['name'],
                                                            mfw_sug[0]['id']), logging.INFO)
                return {'id': mfw_sug[0]['id'], 'name': mfw_sug[0]['name'], 'type': mfw_sug[0]['type']}

        self.log(u'Cannnot match: %s(%s)' % (entry['sname'], entry['surl']), logging.INFO)

    # 通过id拼接图片url
    @staticmethod
    def images_proc(urls):
        from hashlib import md5

        images = []
        for k in urls:
            url = 'http://hiphotos.baidu.com/lvpics/pic/item/%s.jpg' % k
            key = md5(url).hexdigest()
            src = 'baidu'
            images.append({'key': key, 'src': src})

        return images

    # 文本格式的处理
    @staticmethod
    def text_pro(text):
        if text:
            text = filter(lambda val: val, [tmp.strip() for tmp in re.split(r'\n+', text)])
            tmp_text = [u'<p>%s</p>' % tmp for tmp in text]
            return u'<div>%s</div>' % (''.join(tmp_text))
        else:
            return ''

    def proc_traffic(self, data, contents, is_locality):
        # 处理交通
        traffic_intro = ''
        traffic_details = {}

        if 'traffic' in contents:
            traffic_intro = contents['traffic']['desc'] if 'desc' in contents['traffic'] else ''
            for key in ['remote', 'local']:
                traffic = []
                if key in contents['traffic']:
                    for node in contents['traffic'][key]:
                        traffic.append({
                            'title': node['name'],
                            'contents_html': self.text_pro(node['desc']),
                            'contents': node['desc']
                        })
                traffic_details[key + 'Traffic'] = traffic

        if is_locality:
            data['trafficIntro'] = self.text_pro(traffic_intro)
            for key in traffic_details:
                data[key] = []
                for tmp in traffic_details[key]:
                    title = tmp['title']
                    desc = tmp['contents_html']
                    data[key].append({'title': title, 'desc': desc})
        else:
            tmp = [traffic_intro.strip()]
            for value in (traffic_details[t_type] for t_type in ['localTraffic', 'remoteTraffic'] if
                          t_type in traffic_details):
                info_entry = [u'%s：\n\n%s' % (value_tmp['title'], value_tmp['contents']) for value_tmp in value]
                tmp.extend(info_entry)
            tmp = filter(lambda val: val, tmp)
            data['trafficInfo'] = '\n\n'.join(tmp) if tmp else ''

    def proc_misc(self, data, contents, is_locality):
        # 示例：func('shoppingIntro', 'commodities', 'shopping', 'goods')

        info = {}

        def func(h1, h2, t1, t2):
            item_lists = []
            if t1 in contents:
                if 'desc' in contents[t1]:
                    info[h1] = self.text_pro(contents[t1]['desc'])

                if t2 in contents[t1]:
                    for node in contents[t1][t2]:
                        # 图片
                        images = []
                        if 'pic_url' in node:
                            pic_url = node['pic_url'].strip()
                            if pic_url:
                                images = self.images_proc([pic_url])
                        item_lists.append(
                            {'title': node['name'], 'desc': self.text_pro(node['desc']), 'images': images})

            if item_lists:
                info[h2] = item_lists

        # 购物
        func('shoppingIntro', 'commodities', 'shopping', 'goods')
        # 美食
        func('diningIntro', 'cuisines', 'dining', 'food')
        # 活动
        func('activityIntro', 'activities', 'entertainment', 'activity')
        # 小贴士
        func('tipsIntro', 'tips', 'attention', 'list')
        # 地理文化
        func('geoHistoryIntro', 'geoHistory', 'geography_history', 'list')

        if is_locality:
            for k, v in info.items():
                if v:
                    data[k] = v
            data['miscInfo'] = []
        else:
            if info:
                data['miscInfo'] = info

        if not is_locality:
            # 门票信息
            if 'ticket_info' in contents:
                price_desc = contents['ticket_info']['price_desc'] if 'price_desc' in contents['ticket_info'] else ''
                open_time_desc = contents['ticket_info']['open_time_desc'] if 'open_time_desc' in contents[
                    'ticket_info'] else ''
                data['priceDesc'] = price_desc
                data['openTime'] = open_time_desc
            else:
                data['priceDesc'] = ''
                data['openTime'] = ''

    def build_scene(self, data, entry):
        from utils import guess_coords

        col_loc = get_mongodb('geo', 'Locality', 'mongo')
        col_country = get_mongodb('geo', 'Country', 'mongo')

        for k, v in {'abroad': True if entry['is_china'] == '0' else False,
                     'taoziEnabled': False, 'enabled': True,
                     'commentCnt': int(entry['rating_count']) if 'rating_count' in entry else None,
                     'visitCnt': int(entry['gone_count']) if 'gone_count' in entry else None,
                     'favorCnt': int(entry['going_count']) if 'going_count' in entry else None,
                     'rating': float(entry['star']) / 5 if 'star' in entry else None}.items():
            data[k] = v

        # 别名
        alias = set()
        for key in ['sname', 'ambiguity_sname']:
            if key in entry:
                data['zhName'] = entry['sname']  # 中文名
                alias.add(entry[key].strip().lower())
            else:
                continue

        loc_list = []
        # 层级结构
        if 'scene_path' in entry:
            country_fetched = False
            for scene_path in entry['scene_path']:
                if country_fetched:
                    ret = col_loc.find_one({'alias': scene_path['sname']}, {'zhName': 1, 'enName': 1})
                    if ret:
                        loc_list.append({key: ret[key] for key in ['_id', 'zhName', 'enName']})
                else:
                    ret = col_country.find_one({'alias': scene_path['sname']}, {'zhName': 1, 'enName': 1})
                    if ret:
                        data['country'] = {key: ret[key] for key in ['_id', 'zhName', 'enName']}
                        loc_list.append({key: ret[key] for key in ['_id', 'zhName', 'enName']})
                        country_fetched = True

        data['targets'] = [loc_tmp['_id'] for loc_tmp in loc_list]

        data['tags'] = []

        if 'ext' in entry:
            tmp = entry['ext']
            data['desc'] = tmp['more_desc'] \
                if 'more_desc' in tmp else tmp['abs_desc']
            data['rating'] = float(tmp['avg_remark_score']) / 5 \
                if 'avg_remark_score' in tmp else None
            data['enName'] = tmp['en_sname'] if 'en_sname' in tmp else ''
            # 位置信息
            # if 'map_info' in tmp and tmp['map_info']:
            map_info = filter(lambda val: val,
                              [c_tmp for c_tmp in re.split(ur'[,/\uff0c]', tmp['map_info'])])
            try:
                coord = [float(node) for node in map_info]
                if len(coord) == 2:
                    # 有时候经纬度反了
                    ret = guess_coords(*coord)
                    if ret:
                        data['location'] = {'type': 'Point', 'coordinates': ret}
            except (ValueError, UnicodeEncodeError):
                self.log(map_info, logging.ERROR)
        else:
            data['desc'] = ''
            data['rating'] = None
            data['enName'] = ''
            data['location'] = None

        # 设置别名
        if data['enName']:
            alias.add(data['enName'])
        data['alias'] = list(set(filter(lambda val: val, [tmp.strip().lower() for tmp in alias])))

        # 字段
        contents = entry['content'] if 'content' in entry else {}

        # 处理图片
        data['images'] = []
        if 'highlight' in contents:
            if 'list' in contents['highlight']:
                data['images'] = self.images_proc(contents['highlight']['list'])

        if len(data['images']) < 10:
            img_remains = 10 - len(data['images'])
            col = get_mongodb('raw_baidu', 'BaiduImage', 'mongo-raw')
            for tmp in col.find({'sid': entry['sid']}).limit(img_remains):
                img = {'key': tmp['key'], 'src': 'baidu'}
                for k in ['title', 'user']:
                    if k in tmp:
                        img[k] = tmp[k]
                data['images'].append(img)

        if 'tips' in entry and entry['tips']:
            tips = []
            for item in entry['tips']:
                title = item['title']
                desc = '<div>%s</div>' % item['contents']
                tips.append({'title': title, 'desc': desc})
            if tips:
                data['tips'] = tips

        return data

    def build_misc(self, data, entry):
        contents = entry['content'] if 'content' in entry else {}

        is_locality = entry['is_locality']

        # 交通信息
        self.proc_traffic(data, contents, is_locality)

        # 旅行时间
        if 'besttime' in contents:
            best_time = contents['besttime']
            travel_month = best_time['more_desc'] if 'more_desc' in best_time else ''
            if not travel_month:
                travel_month = best_time['simple_desc'] if 'simple_desc' in best_time else ''
            data['travelMonth'] = travel_month.strip()

            tmp_time_cost = best_time['recommend_visit_time'] if 'recommend_visit_time' in best_time else ''
            data['timeCostDesc'] = tmp_time_cost

        self.proc_misc(data, contents, is_locality)

        return data

    @staticmethod
    def build_hotness(entry, col, tot):
        score_list = []
        for k1, k2 in [['commentCnt', 'rating_count'], ['favorCnt', 'going_count'], ['visitCnt', 'gone_count']]:
            if k1 not in entry:
                continue
            score = col.find({k2: {'$lt': entry[k1]}}, {'_id': 1}).count() / float(tot)
            score_list.append(score)

        if not score_list:
            return

        avg = sum(score_list) / len(score_list)

        if 'hotness' in entry and entry['hotness']:
            entry['hotness'] += avg * 0.2 - 0.1
        else:
            entry['hotness'] = avg
        if entry['hotness'] > 1:
            entry['hotness'] = 1
        elif entry['hotness'] < 0:
            entry['hotness'] = 0

    def populate_tasks(self):
        col_mdd = get_mongodb('proc_baidu', 'BaiduLocality', profile='mongo-raw')
        col_vs = get_mongodb('proc_baidu', 'BaiduPoi', profile='mongo-raw')

        col_raw = get_mongodb('raw_baidu', 'BaiduScene', profile='mongo-raw')
        tot = col_raw.count()

        query = json.loads(self.args.query) if self.args.query else {}

        cursor = col_raw.find(query)
        cursor.skip(self.args.skip)
        if self.args.limit:
            cursor.limit(self.args.limit)

        for entry in cursor:
            def func(val=entry):
                self.log(u'Processing: %s, sid=%s, surl=%s' % (val['sname'], val['sid'], val['surl']))

                self.get_type_code(val)
                if 'type_code' not in val:
                    self.logger.error(
                        'Cannot find type code for: %s, sid=%s, surl=%s' % (val['sname'], val['sid'], val['surl']))
                    return

                val['is_locality'] = val['type_code'] <= 5
                data = {'source': {'baidu': {'id': val['sid'], 'surl': val['surl']}}}
                self.build_scene(data, val)

                location = data['location'] if 'location' in data else None

                if self.args.mfw_match:
                    mfw_ret = None
                    if location:
                        mfw_ret = self.match_mfw(val, location)

                    if mfw_ret:
                        data['source']['mafengwo'] = {'id': mfw_ret['id']}
                        alias_set = set(data['alias'])
                        alias_set.add(mfw_ret['name'].strip().lower())
                        data['alias'] = list(alias_set)

                self.build_misc(data, val)

                self.build_hotness(data, col_raw, tot)

                col = col_mdd if val['is_locality'] else col_vs
                source = data.pop('source')
                ops = {'$set': data}
                if 'mafengwo' in source:
                    ops['$set']['source.mafengwo'] = source['mafengwo']

                col.update({'source.baidu.id': source['baidu']['id']}, ops, upsert=True)

            self.add_task(func)

