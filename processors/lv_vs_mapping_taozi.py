# coding=utf-8

from processors import BaseProcessor
from utils.database import get_mongodb
from utils.mixin import BaiduSuggestion
from math import radians, asin, sqrt, cos, sin


class LvVsMappingTaozi(BaseProcessor, BaiduSuggestion):
    """
    匹配桃子旅行和同城两方的景点
    """
    name = 'lv_mapping_taozi'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)
        BaiduSuggestion.__init__(self, *args, **kwargs)

    def vs_generate(self):
        """
        待处理景点生成器
        """
        conn = get_mongodb('raw_ly', 'ViewSpot', 'mongo-raw')
        for entry in list(conn.find({'mapped': False}, {'lyId': 1, 'lyName': 1, 'lat': 1, 'lng': 1})):
            conn.update({'lyId': entry['lyId']}, {'$set': {'mapped': True}}, upsert=False)
            yield entry

    def cal_dist(self, lat1, lng1, lat2, lng2):
        """
        通过经纬度计算两点距离
        """
        EARTH_RADIUS = 6378.137
        radlat1 = radians(lat1)  # a点纬度(单位是弧度)
        radlat2 = radians(lat2)  # b点纬度(单位是弧度)
        a = radlat1 - radlat2  # 两点间的纬度弧度差
        b = radians(lng1) - radians(lng2)  # 两点间的经度弧度差
        s = 2 * asin(sqrt(pow(sin(a/2), 2) + cos(radlat1)*cos(radlat2)*pow(sin(b/2), 2)))  # 两点间的弧度
        s *= EARTH_RADIUS
        s = round(s * 10000) / 10000  # 四舍五入保留小数点后4位
        if s < 0:
            return -s
        else:
            return s

    def look_up_vs(self):
        """
        查询匹配：先做已有库名字匹配，无则通过百度旅游suggestion匹配
        """
        conn_taozi = get_mongodb('poi', 'ViewSpot', 'mongo')
        conn_raw_ly = get_mongodb('raw_ly', 'ViewSpot', 'mongo-raw')
        coon_mapping = get_mongodb('poi', 'LyMapping', 'mongo')

        for vs in self.vs_generate():
            def map_vs(vs_info=vs):
                ly_id = int(vs_info['lyId'])
                ly_name = vs_info['lyName']
                res = conn_taozi.find_one({'alias': ly_name}, {'_id': True, 'zhName': True})
                if res is not None:
                    coon_mapping.update({'itemId': res['_id']}, {'$set': {'itemId': res['_id'], 'zhNameLxp': res['zhName'], 'zhNameLy': ly_name, 'lyId': ly_id}}, upsert=True)
                    conn_raw_ly.update({'lyId': ly_id}, {'$set': {'mapOk': True}}, upsert=False)
                else:
                    suggs = self.get_baidu_sug(ly_name, None)
                    if len(suggs):
                        target = suggs[0]  # 只选第一个
                        if target['type_code'] >= 6 and self.cal_dist(vs_info['lat'], vs_info['lng'], target['lat'], target['lng']) < 50:  # 单位 km
                            res = conn_taozi.find_one({'source.baidu.id': target['sid']}, {'_id': True, 'zhName': True})
                            if res is not None:
                                coon_mapping.update({'itemId': res['_id']}, {'$set': {'itemId': res['_id'], 'zhNameLxp': res['zhName'], 'zhNameLy': ly_name, 'lyId': ly_id, 'mapEstimated': True}}, upsert=True)
                                conn_raw_ly.update({'lyId': ly_id}, {'$set': {'mapEstimated': True}}, upsert=False)
            self.add_task(map_vs)

    def populate_tasks(self):
        self.look_up_vs()