# coding=utf-8
import json
import re
from hashlib import md5

from core import ProcessorEngine

from utils import haversine
from utils.database import get_mongodb


__author__ = 'zephyre'


class BaiduSuggestion(object):
    def get_baidu_sug(self, name, location):
        from utils import mercator2wgs
        from urllib import quote

        url = u'http://lvyou.baidu.com/destination/ajax/sug?wd=%s&prod=lvyou_new&su_num=20' % name

        key = quote(name.encode('utf-8'))

        col = get_mongodb('raw_baidu', 'BaiduSug', 'mongo-raw')
        ret = col.find_one({'key': key}, {'body': 1})
        body = None
        if ret:
            body = ret['body']
        else:
            try:
                response = ProcessorEngine.get_instance().request.get(url)
                if response:
                    body = response.text
                    col.update({'key': key}, {'key': key, 'body': body, 'url': url}, upsert=True)
            except IOError:
                pass
        if not body:
            return []

        try:
            sug = json.loads(json.loads(body)['data']['sug'])
            result = []
            for s in sug['s']:
                tmp = re.split(r'\$', s)
                entry = {'sname': tmp[0].strip(),
                         'parents': tmp[6].strip(),
                         'sid': tmp[8].strip(),
                         'surl': tmp[22].strip(),
                         'parent_sid': tmp[26].strip(),
                         'type_code': int(tmp[24])}

                mx = float(tmp[14])
                my = float(tmp[16])
                entry['lng'], entry['lat'] = mercator2wgs(mx, my)

                result.append(entry)

            return result
        except IOError as e:
            e.message += 'url: %s' % url
            raise e
        except (ValueError, KeyError):
            return []


class MfwSuggestion(object):
    def get_mfw_sug(self, name, sug_type, location):
        """
        Get mafengwo suggestions

        :param sug_type suggetion type. Could be one of the following values: 'mdd', 'vs'
        """
        from urllib import unquote_plus, quote

        url = 'http://www.mafengwo.cn/group/ss.php?callback=j&key=%s' % quote(name.encode('utf-8'))
        key = md5(url).hexdigest()

        col = get_mongodb('raw_mfw', 'MfwSug', 'mongo-raw')
        ret = col.find_one({'key': key}, {'body': 1})
        body = None
        if ret:
            body = ret['body']
        else:
            try:
                response = ProcessorEngine.get_instance().request.get(url)
                if response:
                    body = response.text
                    col.update({'key': key}, {'key': key, 'body': body, 'name': name, 'url': url}, upsert=True)
            except IOError:
                pass

        if not body:
            return []
        rtext = unquote_plus(json.loads(body[2:-2])['data'].encode('utf-8')).decode('utf-8')

        # j('search://|mdd|/group/cs.php?t=%E6%90%9C%E7%B4%A2%E7%9B%B4%E8%BE%BE&p=mdd&l=%2Ftravel-scenic-spot%2F
        # mafengwo%2F11124.html&d=%E4%BC%A6%E6%95%A6|ss-place|伦敦|英格兰|伦敦|search://|gonglve|/group/cs.php?t=
        # %E6%90%9C%E7%B4%A2%E7%9B%B4%E8%BE%BE&p=gonglve&l=%2Fgonglve%2Fmdd-11124.html&d=%E4%BC%A6%E6%95%A6%E6%97
        # %85%E6%B8%B8%E6%94%BB%E7%95%A5|ss-gonglve|伦敦旅游攻略|311730下载|伦敦旅游攻略|search://|gonglve|/group/cs.php?
        # t=%E6%90%9C%E7%B4%A2%E7%9B%B4%E8%BE%BE&p=gonglve&l=%2Fgonglve%2Fzt-423.html&d=%E4%BC%A6%E6%95%A6%E5%B0%8F%E5
        # %BA%97%E6%94%BB%E7%95%A5|ss-gonglve|伦敦小店攻略|79716下载|伦敦小店攻略|search://|hotel|/group/cs.php?t=%E6%90
        # %9C%E7%B4%A2%E7%9B%B4%E8%BE%BE&p=hotel&l=%2Fhotel%2F11124%2F&d=%E4%BC%A6%E6%95%A6%E9%85%92%E5%BA%97
        # |ss-hotel|伦敦酒店|2304间<i class="ico-new"></i>|伦敦酒店|search://|wenda|/group/cs.php?t=%E6%90%9C%E7%B4%A2
        # %E7%9B%B4%E8%BE%BE&p=wenda&l=%2Fwenda%2Farea-11124.html&d=%E4%BC%A6%E6%95%A6%E9%97%AE%E7%AD%94
        # |ss-ask|伦敦问答|143条|伦敦问答|search://|scenic|/group/cs.php?t=%E6%90%9C%E7%B4%A2%E7%9B%B4%E8%BE%BE
        # &p=scenic&l=%2Fjd%2F11124%2Fgonglve.html&d=%E4%BC%A6%E6%95%A6%E6%99%AF%E7%82%B9
        # |ss-scenic|伦敦景点|108个|伦敦景点|search://|tsms|/group/cs.php?t=%E6%90%9C%E7%B4%A2%E7%9B%B4%E8%BE%BE&p=tsms
        # &l=%2Fcy%2F11124%2Ftese.html&d=%E4%BC%A6%E6%95%A6%E7%89%B9%E8%89%B2%E7%BE%8E%E9%A3%9F
        # |ss-cate|伦敦特色美食|5条|伦敦特色美食|search://|mdd|/group/cs.php?t=%E6%90%9C%E7%B4%A2%E7%9B%B4%E8%BE%BE
        # &p=mdd&l=%2Ftravel-scenic-spot%2Fmafengwo%2F60657.html&d=%E4%BC%A6%E6%95%A6%E5%BE%B7%E9%87%8C
        # |ss-place|伦敦德里|北爱尔兰|伦敦德里|search://|mdd|/group/cs.php?t=%E6%90%9C%E7%B4%A2%E7%9B%B4%E8%BE%BE&p=mdd
        # &l=%2Ftravel-scenic-spot%2Fmafengwo%2F134570.html&d=%E4%BC%A6%E6%95%A6%28%E5%AE%89%E5%A4%A7%E7%95%A5%E7%9C
        # %81%29|ss-place|伦敦(安大略省)|安大略省|伦敦(安大略省)|search://|user|/group/s.php?q=%E4%BC%A6%E6%95%A6&t=user
        # |ss-user|伦敦|274个|搜&quot;伦敦&quot;相关用户|search://|more|/group/s.php?q=%E4%BC%A6%E6%95%A6||伦敦||
        # 查看&quot;伦敦&quot;更多搜索结果')

        tmpl = {'mdd': {'title': r'\|mdd\|',
                        'id': r'/travel-scenic-spot/mafengwo/(\d+)\.html',
                        'name': r'&d=([^\|&]+)'},
                'vs': {'title': r'\|scenic\|',
                       'id': r'/poi/(\d+)\.html',
                       'name': r'&d=([^\|&]+)'}}

        assert sug_type in ['mdd', 'vs']

        col_mfw_mdd = get_mongodb('raw_mfw', 'MafengwoMdd', 'mongo-raw')
        col_mfw_vs = get_mongodb('raw_mfw', 'MafengwoVs', 'mongo-raw')

        if sug_type == 'mdd':
            tmpl_list = [tmpl['mdd'], tmpl['vs']]
            type_list = ['mdd', 'vs']
            col_list = [col_mfw_mdd, col_mfw_vs]
        else:
            tmpl_list = [tmpl['vs'], tmpl['mdd']]
            type_list = ['vs', 'mdd']
            col_list = [col_mfw_vs, col_mfw_mdd]

        results = []
        coords = location['coordinates']

        for r in filter(lambda val: re.search(r'\|(mdd|scenic)\|', val), re.split(r'search://', rtext)):

            for idx, t in enumerate(tmpl_list):
                match = re.search(t['id'], r)
                if match:
                    rid = int(match.group(1))

                    col = col_list[idx]
                    ret = col.find_one({'id': rid}, {'title': 1, 'lat': 1, 'lng': 1})
                    if not ret:
                        ret = self.poi_info(rid)
                    if not ret:
                        continue

                    name = ret['title']

                    try:
                        lat = float(ret['lat'])
                        lng = float(ret['lng'])
                    except (KeyError, ValueError, TypeError):
                        continue

                    dist = haversine(lng, lat, coords[0], coords[1])
                    if dist < 400:
                        results.append({'id': rid, 'name': name, 'type': type_list[idx], 'lat': lat, 'lng': lng,
                                        'dist': dist})
        return results

    @staticmethod
    def poi_info(poi_id):
        """
        Get additional information of a POI

        :param poi_id:
        :return:
        """

        from lxml import etree

        try:
            body = None
            col = get_mongodb('raw_mfw', 'MfwPoiBody', 'mongo-raw')
            ret = col.find_one({'key': poi_id}, {'body': 1})
            if ret:
                body = ret['body']
            else:
                url = 'http://www.mafengwo.cn/poi/%d.html' % poi_id
                response = ProcessorEngine.get_instance().request.get(url)
                if response:
                    body = response.text
                    col.update({'key': poi_id}, {'key': poi_id, 'body': body}, upsert=True)
            if not body:
                return

            tmp = re.search(r'window\.Env\s*=\s*\{(.+?)\}\s*;', body)
            if not tmp:
                raise ValueError('No map info found: %d' % poi_id)
            loc_data = json.loads('{%s}' % tmp.group(1))
            lat = loc_data['lat']
            lng = loc_data['lng']

            tree = etree.fromstring(body, etree.HTMLParser())
            title = unicode(tree.xpath('//div[@class="col-main"]//div[contains(@class,"title")]'
                                       '/div[@class="t"]/h1/text()')[0])
            return {'lat': lat, 'lng': lng, 'title': title}
        except IOError:
            return