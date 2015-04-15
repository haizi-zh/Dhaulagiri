# coding=utf-8

import json
import logging
import re
from hashlib import md5

from lxml.sax import ElementTreeContentHandler

from processors_old import BaseProcessor
from utils import haversine
from utils.database import get_mongodb
from utils.mixin import BaiduSuggestion, MfwSuggestion


__author__ = 'zephyre'


class MfwImageExtractor(object):
    def __init__(self):
        from hashlib import md5

        def helper(image_id, src):
            key = md5(src).hexdigest()
            url = 'http://aizou.qiniudn.com/%s' % key

            return {'id': image_id, 'metadata': {}, 'src': src, 'url': url, 'key': key, 'url_hash': key}

        def f1(src):
            pattern = r'([^\./]+)\.\w+\.[\w\d]+\.(jpeg|bmp|png)$'
            match = re.search(pattern, src)
            if not match:
                return None
            c = match.group(1)
            ext = match.group(2)
            src = re.sub(pattern, '%s.%s' % (c, ext), src)
            return helper(c, src)

        self.extractor = [f1]

    def retrieve_image(self, src):
        for func in self.extractor:
            ret = func(src)
            if ret:
                return ret


class SuggestionProcessor(BaseProcessor):
    """
    读取蚂蜂窝的输入提示
    """
    name = 'mfw-sug'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)
        self.args = self.args_builder()

    def args_builder(self):
        parser = self.arg_parser
        parser.add_argument('--limit', default=None, type=int)
        parser.add_argument('--skip', default=0, type=int)
        parser.add_argument('--query', type=str)
        args, leftover = parser.parse_known_args()
        return args

    def populate_tasks(self):
        from urllib import quote

        col_raw1 = get_mongodb('raw_baidu', 'BaiduPoi', 'mongo-raw')
        col_raw2 = get_mongodb('raw_baidu', 'BaiduLocality', 'mongo-raw')

        col = get_mongodb('raw_mfw', 'MfwSug', 'mongo-raw')

        query = json.loads(self.args.query) if self.args.query else {}

        for col_raw in [col_raw1, col_raw2]:
            cursor = col_raw.find(query, {'ambiguity_sname': 1, 'sname': 1, 'sid': 1}).skip(self.args.skip)
            if self.args.limit:
                cursor.limit(self.args.limit)

            for val in cursor:
                def func(entry=val):

                    for name in set(filter(lambda v: v.strip(), [entry[k] for k in ['ambiguity_sname', 'sname']])):
                        self.log(u'Parsing: %s, id=%s' % (name, entry['sid']))

                        url = 'http://www.mafengwo.cn/group/ss.php?callback=j&key=%s' % quote(name.encode('utf-8'))
                        key = md5(url).hexdigest()

                        if col.find_one({'key': key}, {'_id': 1}):
                            # The record already exists
                            self.log(u'Already exists, skipping: %s, id=%s' % (name, entry['sid']))
                            continue

                        response = self.request.get(url)
                        if not response:
                            self.log(u'Failed to query url: %s, %s, id=%s' % (url, name, entry['sid']), logging.ERROR)
                            continue

                        col.update({'key': key}, {'key': key, 'body': response.text, 'name': name, 'url': url},
                                   upsert=True)

                self.add_task(func)


class PoiCommentProcessor(BaseProcessor, MfwImageExtractor):
    """
    清洗蚂蜂窝的POI评论数据
    """
    name = 'mfw-poi-comment'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)
        MfwImageExtractor.__init__(self)
        self.args = self.args_builder()

    def args_builder(self):
        parser = self.arg_parser
        parser.add_argument('--limit', default=None, type=int)
        parser.add_argument('--skip', default=0, type=int)
        args, leftover = parser.parse_known_args()
        return args

    def populate_tasks(self):
        col = get_mongodb('raw_mfw', 'MafengwoComment', 'mongo-raw')
        col_vs = get_mongodb('poi', 'ViewSpot', 'mongo')
        col_dining = get_mongodb('poi', 'Restaurant', 'mongo')
        col_shopping = get_mongodb('poi', 'Shopping', 'mongo')

        cursor = col.find({}, snapshot=True)
        cursor.skip(self.args.skip)
        if self.args.limit:
            cursor.limit(self.args.limit)

        poi_dict = {}

        for val in cursor:
            def func(entry=val):
                poi_dbs = {'vs': col_vs, 'dining': col_dining, 'shopping': col_shopping}

                def fetch_poi_item(mfw_id, poi_type):
                    if mfw_id in poi_dict:
                        return poi_dict[mfw_id]
                    else:
                        col_poi = poi_dbs[poi_type]
                        tmp = col_poi.find_one({'source.mafengwo.id': mfw_id}, {'_id': 1})
                        if tmp:
                            ret = {'type': poi_type, 'item_id': tmp['_id']}
                        else:
                            self.log('Failed to find POI: %d' % entry['poi_id'], logging.DEBUG)
                            ret = None
                        poi_dict[mfw_id] = ret
                        return ret

                ret = None
                for v in ['vs', 'dining', 'shopping']:
                    ret = fetch_poi_item(entry['poi_id'], v)
                    if ret:
                        break

                if not ret:
                    return

                self.log('Parsing comment for %s: %s(%d)' % (ret['type'], ret['item_id'], entry['poi_id']))
                for item_type, item_data in self.parse_contents(entry['contents']):
                    if item_type != 'image':
                        item_data['source'] = {'mafengwo': {'id': entry['comment_id']}}
                        item_data['type'] = ret['type']
                        item_data['itemId'] = ret['item_id']

                    self.update(item_type, item_data)

            self.add_task(func)


    @staticmethod
    def update(item_type, item_data):
        if item_type == 'comment':
            db_dict = {'vs': 'ViewSpotComment', 'dining': 'DiningComment', 'shopping': 'ShoppingComment'}
            db_name = db_dict[item_data.pop('type')]
            col = get_mongodb('comment', db_name, 'mongo')
            col.update({'source.mafengwo.id': item_data['source']['mafengwo']['id']}, {'$set': item_data}, upsert=True)
        elif item_type == 'image':
            col = get_mongodb('imagestore', 'ImageCandidates', 'mongo')
            col.update({'key': item_data['key']}, {'$set': item_data}, upsert=True)
        else:
            assert False, 'Invalid type: %s' % item_type

    def parse(self, entry):
        col_vs = get_mongodb('poi', 'ViewSpot', 'mongo')
        col_dining = get_mongodb('poi', 'Restaurant', 'mongo')
        col_shopping = get_mongodb('poi', 'Shopping', 'mongo')
        poi_dbs = {'vs': col_vs, 'dining': col_dining, 'shopping': col_shopping}

        def fetch_poi_item(mfw_id, poi_type):
            col_poi = poi_dbs[poi_type]
            tmp = col_poi.find_one({'source.mafengwo.id': mfw_id}, {'_id': 1})
            if tmp:
                return {'type': poi_type, 'item_id': tmp['_id']}
            else:
                return None

        ret = None
        for v in ['vs', 'dining', 'shopping']:
            ret = fetch_poi_item(entry['poi_id'], v)
            if ret:
                break

        if not ret:
            return

        for item_type, item_data in self.parse_contents(entry['contents']):
            if item_type != 'image':
                item_data['source'] = {'mafengwo': {'id': entry['comment_id']}}
                item_data['type'] = ret['type']
                item_data['itemId'] = ret['item_id']

            self.update(item_type, item_data)

    def parse_contents(self, node):
        from lxml import etree
        from datetime import datetime, timedelta

        sel = etree.fromstring(node, parser=etree.HTMLParser())
        avatar = sel.xpath('//span[@class="user-avatar"]/a[@href]/img[@src]/@src')[0]
        ret = self.retrieve_image(avatar)

        if ret:
            # 检查是否已经存在于数据库中
            col_im = get_mongodb('imagestore', 'Images', 'mongo')
            col_cand = get_mongodb('imagestore', 'ImageCandidates', 'mongo')

            img = col_im.find_one({'url_hash': ret['url_hash']}, {'_id': 1})
            if not img:
                img = col_cand.find_one({'url_hash': ret['url_hash']}, {'_id': 1})
            if not img:
                # 添加到待抓取列表中
                data = {'key': ret['key'], 'url': ret['src'], 'url_hash': ret['url_hash']}
                item_type = 'image'
                yield item_type, data

            avatar = ret['key']
        else:
            avatar = ''

        tmp = sel.xpath('//div[@class="info"]/a[@class="user-name"]/text()')
        user = tmp[0] if tmp else ''

        tmp = sel.xpath('//span[@class="useful-num"]/text()')
        try:
            vote_cnt = int(tmp[0])
        except (ValueError, IndexError):
            vote_cnt = 0

        paras = []
        for content in sel.xpath('//div[@class="c-content"]/p'):
            tmp = ''.join(content.itertext()).strip()
            if tmp:
                paras.append(tmp)
        contents = '\n\n'.join(paras)

        time_str = sel.xpath('//span[@class="time"]/text()')[0]
        ts = long((datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S') - timedelta(seconds=8 * 3600)
                   - datetime.utcfromtimestamp(0)).total_seconds() * 1000)

        data = {'authorName': user, 'authorAvatar': avatar, 'publishTime': ts, 'voteCnt': vote_cnt,
                'contents': contents}
        item_type = 'comment'
        yield item_type, data


class MfwHtmlHandler(ElementTreeContentHandler):
    def startElementNS(self, ns_name, qname, attributes=None):
        from urlparse import urlparse

        if qname == 'a':  # and attributes.has_key('href'):
            attrs = getattr(attributes, '_attrs')
            new_attrs = {}
            for key, value in attrs.items():
                if key[1] == 'href':
                    ret = urlparse(value)
                    if not ret.netloc or 'mafengwo' in ret.netloc:
                        # remove links that point to mafengwo sites
                        continue
                new_attrs[key] = value
            setattr(attributes, '_attrs', new_attrs)

        ElementTreeContentHandler.startElementNS(self, ns_name, qname, attributes)


class MafengwoProcessor(BaseProcessor, BaiduSuggestion, MfwSuggestion):
    """
    马蜂窝目的地的清洗

    参数列表：
    def-hot：默认的热度。默认值为0.3。
    denom：计算热度/评分等的基准因子。默认值为2000。
    lower/upper：分片处理。默认为不分片。
    slice：分片处理的步进。默认为8。
    mdd/vs/gw/cy/...：处理目的地/景点/购物/餐饮等。
    limit：条数限制。
    query：通过查询条件限制处理对象。
    bind-baidu：是否和百度进行绑定。默认为不绑定。
    """

    name = 'mfw-mdd'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)
        self.args = self.args_builder()

    def args_builder(self):
        parser = self.arg_parser
        parser.add_argument('--limit', type=int)
        parser.add_argument('--skip', default=0, type=int)
        parser.add_argument('--query', type=int)
        parser.add_argument('--baidu-match', action='store_true')
        parser.add_argument('--type', choices=['mdd', 'vs'], required=True)
        args, leftover = parser.parse_known_args()
        return args

    @staticmethod
    def is_chn(text):
        """
        是否为中文
        判断算法：至少出现一个中文字符
        :param text:
        """
        for c in text:
            if 0x9fff >= ord(c) >= 0x4e00 and ord(c):
                return True

        return False

    @staticmethod
    def is_eng(text):
        for c in text:
            if ord(c) < 32 or ord(c) > 126:
                return False
        return True

    def parse_name(self, name):
        name = name.strip()
        term_list = []

        # 处理括号
        match = re.search(ur'([^\(\)]+)[\(（]([^\(\)]+)[\)）]', name)
        if match:
            term_list.extend([match.group(1), match.group(2)])
        if not term_list:
            term_list = [name]

        name_list = []
        for term in term_list:
            # 处理/的情况
            tmp = filter(lambda val: val,
                         [re.sub(r'\s+', ' ', tmp.strip(), flags=re.U) for tmp in re.split(r'/', term)])
            if not tmp:
                continue
            name_list.extend(tmp)

        # 名称推测算法：从前往后测试。
        # 第一个至少含有一个中文，且可能包含简单英语及数字的term，为zhName。
        # 第一个全英文term，为enName。
        # 第一个既不是zhName，也不是enName的，为localName

        # 优先级
        # zhName: zhName > enName > localName
        # enName: enName > localName
        # localName: localName

        zh_name = None
        en_name = None
        loc_name = None
        for tmp in name_list:
            tmp = tmp.strip()
            if not zh_name and self.is_chn(tmp):
                zh_name = tmp
            elif not en_name and self.is_eng(tmp):
                en_name = tmp
            elif not loc_name:
                loc_name = tmp

        result = {}
        if zh_name:
            result['zhName'] = zh_name
        elif en_name:
            result['zhName'] = en_name
        else:
            result['zhName'] = loc_name

        if en_name:
            result['enName'] = en_name

        if loc_name:
            result['locName'] = loc_name

        alias = {name.lower()}
        for tmp in name_list:
            alias.add(tmp.lower())

        result['alias'] = list(alias)
        return result

    @staticmethod
    def resolve_targets(item):
        data = item['data']

        col_mdd = get_mongodb('geo', 'Locality', 'mongodb-general')
        col_country = get_mongodb('geo', 'Country', 'mongodb-general')

        country_flag = False
        crumb_list = data.pop('crumbIds')
        crumb = []
        for cid in crumb_list:
            ret = col_mdd.find_one({'source.mafengwo.id': cid}, {'_id': 1, 'zhName': 1, 'enName': 1})
            if not ret and not country_flag:
                ret = col_country.find_one({'source.mafengwo.id': cid}, {'_id': 1, 'zhName': 1, 'enName': 1, 'code': 1})
                if ret:
                    # 添加到country字段
                    data['country'] = ret
                    for key in ret:
                        data['country'][key] = ret[key]
                    country_flag = True
            if ret:
                crumb.append(ret['_id'])
        data['targets'] = crumb

        # 从crumb的最后开始查找。第一个目的地即为city
        city = None
        for idx in xrange(len(crumb_list) - 1, -1, -1):
            cid = crumb_list[idx]
            ret = col_mdd.find_one({'source.mafengwo.id': cid}, {'_id': 1, 'zhName': 1, 'enName': 1})
            if ret:
                city = {'_id': ret['_id']}
                for key in ['zhName', 'enName']:
                    if key in ret:
                        city[key] = ret[key]
                break

        if city:
            data['locality'] = city

    @staticmethod
    def get_plain(body_list):
        """
        将body_list中的内容，作为纯文本格式输出
        """
        from lxml import etree

        if not hasattr(body_list, '__iter__'):
            body_list = [body_list]

        plain_list = [''.join(etree.fromstring(body, parser=etree.HTMLParser()).itertext()).strip() for body in
                      body_list]

        return '\n\n'.join(plain_list) if plain_list else None

    @staticmethod
    def get_html(body_list):
        from lxml import etree
        import lxml.sax

        if not hasattr(body_list, '__iter__'):
            body_list = [body_list]

        proc_list = []

        for body in body_list:
            body = body.replace('\r\n', '\n')
            handler = MfwHtmlHandler()

            tree = etree.fromstring(body, parser=etree.HTMLParser())
            div_list = list(tree[0])
            if len(div_list) > 1:
                tree = etree.Element('div')
                for div_node in div_list:
                    tree.append(div_node)
            else:
                tree = div_list[0]

            lxml.sax.saxify(tree, handler)
            proc_list.append(etree.tostring(handler.etree, encoding='utf-8'))

        if proc_list:
            return '<div>%s</div>' % '\n'.join(proc_list) if len(proc_list) > 1 else proc_list[0]
        else:
            return None

    def parse_vs_contents(self, entry, data):
        """
        解析POI的详细内容
        :param entry:
        :param data:
        :return:
        """
        desc = None
        address = None
        tel = None
        traffic = None
        misc = []
        en_name = None

        for info_entry in entry['desc']:
            if info_entry['name'] == u'简介':
                desc = self.get_plain(info_entry['contents'])
            elif info_entry['name'] == u'地址':
                address = self.get_plain(info_entry['contents'])
            elif info_entry['name'] == u'英文名称':
                en_name = self.get_plain(info_entry['contents'])
            elif info_entry['name'] == u'电话':
                tel = self.get_plain(info_entry['contents'])
            elif info_entry['name'] == u'交通':
                traffic = self.get_plain(info_entry['contents'])
            else:
                misc.append('%s\n\n%s' % (info_entry['name'], self.get_plain(info_entry['contents'])))

        if desc:
            data['desc'] = desc
        if misc:
            data['details'] = '\n\n'.join(misc)
        if address:
            data['address'] = address
        if tel:
            data['tel'] = tel
        if traffic:
            data['trafficInfo'] = traffic
        if en_name:
            data['enName'] = en_name

    def parse_mdd_contents(self, entry, data):
        """
        解析目的地的详细内容
        :param entry:
        :param data:
        :return:
        """
        desc = None
        travel_month = None
        time_cost = None

        local_traffic = []
        remote_traffic = []
        misc_info = []
        activities = []
        specials = []

        for info_entry in entry['contents']:
            if info_entry['info_cat'] == u'概况' and info_entry['title'] == u'简介':
                desc = self.get_plain(info_entry['details'])
            elif info_entry['info_cat'] == u'概况' and info_entry['title'] == u'最佳旅行时间':
                travel_month = self.get_plain(info_entry['details'])
            elif info_entry['info_cat'] == u'概况' and info_entry['title'] == u'建议游玩天数':
                time_cost = self.get_plain(info_entry['details'])
            elif info_entry['info_cat'] == u'内部交通':
                tmp = self.get_html(info_entry['details'])
                if tmp:
                    local_traffic.append({'title': info_entry['title'], 'desc': tmp})
            elif info_entry['info_cat'] == u'外部交通':
                tmp = self.get_html(info_entry['details'])
                if tmp:
                    remote_traffic.append({'title': info_entry['title'], 'desc': tmp})
            elif info_entry['info_cat'] == u'节庆':
                tmp = self.get_html(info_entry['details'])
                if tmp:
                    activities.append({'title': info_entry['title'], 'desc': tmp})
            elif info_entry['info_cat'] == u'亮点':
                tmp = self.get_html(info_entry['details'])
                if tmp:
                    specials.append({'title': info_entry['title'], 'desc': tmp})
            else:
                # 忽略出入境信息
                if info_entry['info_cat'] == u'出入境':
                    continue
                tmp = self.get_html(info_entry['details'])
                if tmp:
                    misc_info.append({'title': info_entry['title'], 'desc': tmp})
        if desc:
            data['desc'] = desc
        if travel_month:
            data['travelMonth'] = travel_month
        if time_cost:
            data['timeCostDesc'] = time_cost
        if local_traffic:
            data['localTraffic'] = local_traffic
        if remote_traffic:
            data['remoteTraffic'] = remote_traffic
        if misc_info:
            data['miscInfo'] = misc_info
        if activities:
            data['activities'] = activities
        if specials:
            data['specials'] = specials

    def retrieve_loc(self, mfw_id):
        """
        有些数据在抓取的时候，没有抓到经纬度。补齐

        :param mfw_type:
        :param mfw_id:
        :return:
        """
        col = get_mongodb('raw_mfw', 'MfwMddBody', 'mongo-raw')
        ret = col.find_one({'key': mfw_id}, {'body': 1})
        if ret:
            body = ret['body']
        else:
            self.logger.debug('Cache missed for mdd: %d' % mfw_id)
            url = ''
            try:
                url = 'http://www.mafengwo.cn/travel-scenic-spot/mafengwo/%d.html' % mfw_id
                response = self.engine.request.get(url)
                body = response.text
                col.update({'key': mfw_id}, {'key': mfw_id, 'body': body}, upsert=True)
            except IOError:
                self.logger.error('Error downloading %s' % url)
                return

        # 网页格式分两种情况：
        # 1. 普通：http://www.mafengwo.cn/jd/10035/gonglve.html
        # 2. 重点目的地：http://www.mafengwo.cn/travel-scenic-spot/mafengwo/11025.html

        from lxml import etree

        tree = etree.fromstring(body, etree.HTMLParser())

        lat = None
        lng = None
        for tmp in tree.xpath('//script[@type="text/javascript"]/text()'):
            m = re.search(r'^\s*var\s+mdd_center(.+$)', tmp, re.M)
            if not m:
                continue
            m_lat = re.search(r"lat:parseFloat\('(\d+.\d+)'\)", m.group(1))
            m_lng = re.search(r"lng:parseFloat\('(\d+.\d+)'\)", m.group(1))
            if m_lat and m_lng:
                lat = float(m_lat.group(1))
                lng = float(m_lng.group(1))
                break

        if not lat or not lng:
            """
                    var map = {
                'zoom' : 0,// || 0,
                'lat'  : 35.179876820661,
                'lng'  : 129.07412052155
            },
            """
            for tmp in tree.xpath('//script[@type="text/javascript"]/text()'):
                m = re.search(r'var\s+map\s+=\s+\{(.+?)\}', tmp, re.S)
                if not m:
                    continue
                m_lat = re.search(r'lat.*?:.*?(\d+\.\d+)', m.group(1))
                m_lng = re.search(r'lng.*?:.*?(\d+\.\d+)', m.group(1))
                if m_lat and m_lng:
                    lat = float(m_lat.group(1))
                    lng = float(m_lng.group(1))
                    break

        return {'type': 'Point', 'coordinates': [lng, lat]} if lat and lng else None

    def populate_tasks(self):
        col_raw = get_mongodb('raw_mfw', 'MafengwoMdd' if self.args.type == 'mdd' else 'MafengwoVs', 'mongo-raw')
        col_raw_im = get_mongodb('raw_mfw', 'MafengwoImage', 'mongo-raw')
        col_country = get_mongodb('geo', 'Country', 'mongo')
        col_proc = get_mongodb('proc_mfw', 'MafengwoMdd' if self.args.type == 'mdd' else 'MafengwoVs', 'mongo-raw')

        tot_num = col_raw.find({}).count()

        cursor = col_raw.find(json.loads(self.args.query) if self.args.query else {})
        if self.args.limit:
            cursor.limit(self.args.limit)
        cursor.skip(self.args.skip)

        # Cache for hotness calculation results
        hotness_cache = {}

        for val in cursor:
            def func(entry=val):
                self.log('Parsing: %s, id=%d' % (entry['title'], entry['id']), logging.DEBUG)
                data = {}

                tmp = self.parse_name(entry['title'])
                if not tmp:
                    self.log('Failed to get names for id=%d' % entry['id'], logging.ERROR)
                    return

                for key in ['enName', 'zhName', 'locName']:
                    if key in tmp:
                        data[key] = tmp[key]

                alias = set([])
                # 去除名称中包含国家的条目
                for a in tmp['alias']:
                    c = col_country.find_one({'alias': a}, {'_id': 1})
                    if not c:
                        alias.add(a)
                data['alias'] = list(alias)

                if 'tags' in entry:
                    data['tags'] = list(set(filter(lambda val: val, [tmp.lower().strip() for tmp in entry['tags']])))

                # 热门程度
                if 'comment_cnt' in entry:
                    data['commentCnt'] = entry['comment_cnt']
                if 'vs_cnt' in entry:
                    data['visitCnt'] = entry['vs_cnt']

                # 计算hotness
                def calc_hotness(key):
                    if key not in entry:
                        return 0.5
                    x = entry[key]
                    sig = '%s:%d' % (key, x)
                    if sig not in hotness_cache:
                        hotness_cache[sig] = col_raw.find({key: {'$lt': x}}).count() / float(tot_num)
                    return hotness_cache[sig]

                hotness_terms = map(calc_hotness, ('comment_cnt', 'images_tot', 'vs_cnt'))
                data['hotness'] = sum(hotness_terms) / float(len(hotness_terms))

                crumb_ids = []
                for crumb_entry in entry['crumb']:
                    if isinstance(crumb_entry, int):
                        cid = crumb_entry
                    else:
                        cid = int(re.search(r'travel-scenic-spot/mafengwo/(\d+)\.html', crumb_entry['url']).group(1))
                    if cid not in crumb_ids:
                        crumb_ids.append(cid)

                data['crumbIds'] = crumb_ids

                data['source'] = {'mafengwo': {'id': entry['id']}}

                if 'lat' in entry and 'lng' in entry:
                    data['location'] = {'type': 'Point', 'coordinates': [entry['lng'], entry['lat']]}
                else:
                    if self.args.type == 'mdd':
                        tmp = self.retrieve_loc(entry['id'])
                        if tmp:
                            data['location'] = tmp
                    else:
                        tmp = self.poi_info(entry['id'])
                        if tmp:
                            data['location'] = {'type': 'Point', 'coordinates': [tmp['lng'], tmp['lat']]}

                # 获得对应的图像
                sig = 'MafengwoMdd-%d' % data['source']['mafengwo']['id']
                image_list = [{'key': md5(tmp['url']).hexdigest()} for tmp in
                              col_raw_im.find({'itemIds': sig}).limit(10)]
                if image_list:
                    data['images'] = image_list

                if self.args.type == 'mdd':
                    self.parse_mdd_contents(entry, data)
                else:
                    self.parse_vs_contents(entry, data)

                if self.args.baidu_match:
                    if 'location' in data:
                        coords = data['location']['coordinates']
                        ret = self.get_baidu_sug(data['zhName'], coords)
                        if not ret:
                            ret = []

                        for val in ret:
                            val['dist'] = haversine(coords[0], coords[1], val['lng'], val['lat'])

                        ret = filter(lambda val: val['sname'] == data['zhName'] and \
                                                 (5 >= val['type_code'] >= 3 if self.args.type == 'mdd'
                                                  else val['type_code'] >= 5)
                                                 and val['dist'] < 400 if self.args.type == 'mdd' else 200, ret)
                        ret = sorted(ret, key=lambda val: (val['type_code'], val['dist']))
                        if ret:
                            data['source']['baidu'] = {'id': ret[0]['sid'], 'surl': ret[0]['surl']}
                            self.log('Matched: %s => %s' % (data['zhName'], ret[0]['sname']))

                    if 'baidu' not in data['source']:
                        self.log('Not matched: %s' % data['zhName'])

                self.log('Parsing done: %s / %s / %s' % tuple(data[key] if key in data else None for key in
                                                              ['zhName', 'enName', 'locName']))

                col_proc.update({'source.mafengwo.id': data['source']['mafengwo']['id']}, {'$set': data},
                                upsert=True)

            self.add_task(func)