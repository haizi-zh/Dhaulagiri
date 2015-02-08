# coding=utf-8

from processors import BaseProcessor
from utils.database import get_mongodb

# TODO 抽取转换动作
class CoordTransform(object):
    def perform(self):
        """
        Program to interfaces, not to implementation.
        """
        pass


# TODO 任务入口
class EntryIterator(object):
    def next_entry(self):
        raise NotImplementedError


# TODO 百度配置文档
class BaiduSceneIterator(EntryIterator):
    def next_entry(self):
        pass


class Coordings(BaseProcessor):

    name = 'coordinate'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)
        self.bdurl = 'http://api.map.baidu.com/geoconv/v1/'
        self.gspurl = 'http://api.gpsspg.com/convert/latlng/'
        self.bdak = '7P5mAce1fZubQOahgDTCAWHo'
        self.gspak = 'EBA5F8F9D7E5CA7E8488EC7B99DFEA23'
        self.gspoid = 506
        self.mongoconn = None
        self.http_error_count = 0
        self.error_limit = 20
        self.to_do_list = []

    def baidu_mc_to_ll(self, latlnglist):
        """
        百度米制坐标转换为百度经纬度坐标
        :param latlnglist: ['lat+lng', ...]
        :return: ['lat+lng', ...]
        """
        if 0 == len(latlnglist):
            return []
        latlngstr = ';'.join(latlnglist)
        querys = {'ak': self.bdak, 'from': 6, 'to': 5, 'coords': latlngstr}
        res = self.request.get(self.bdurl, params=querys).json()
        ll_latlngs = []
        if '0' == str(res['status']):
            latlngs = res['result']
            for latlng in latlngs:
                ll_latlngs.append(str(latlng['y']) + ',' + str(latlng['x']))
        else:
            self.logger.warn("error_mc_ll")
            self.error_count += 1
        return ll_latlngs


    def baidu_ll_to_google(self, latlnglist):
        """
        百度经纬度坐标转换为谷歌地图坐标
        :param latlnglist: ['lat+lng', ...]
        :return: ['lat+lng', ...]
        """
        if 0 == len(latlnglist):
            return []
        latlngstr = ';'.join(latlnglist)
        querys = {'key': self.gspak, 'oid': self.gspoid, 'from': 2, 'to': 1, 'latlng': latlngstr}
        res = self.request.get(self.gspurl, params=querys).json()
        ll_latlngs = []
        if '200' == str(res['status']):
            latlngs = res['result']
            for latlng in latlngs:
                ll_latlngs.append(str(latlng['lat']) + ',' + str(latlng['lng']))
        else:
            self.logger.warn("error_bd_google")
            self.error_count += 1
        return ll_latlngs


    def get_latlngs(self, sp, page_num=20):
        """
        从数据库中获取数据
        :param sp: page index
        :param page_num: 一次处理的数据内容
        :return: _id列表及对应的经纬度列表
        """
        idlist = []
        latlngs = []
        for entry in self.to_do_list[int(sp) * page_num : int(sp+1) * page_num]:
            if float(entry['ext']['map_x']) > 1.0 and float(entry['ext']['map_y']) > 1.0:
                idlist.append(entry['_id'])
                latlngs.append(entry['ext']['map_x'] + ',' + entry['ext']['map_y'])
        return idlist, latlngs


    def update_latlngs(self, idlist, latlngs):
        """
        跟新经纬度
        :param idlist:
        :param latlngs:
        :return: 更新，插入glat和glng字段
        """
        for key, val in enumerate(idlist):
            latlng = latlngs[key].split(',')
            x = float(latlng[0])
            y = float(latlng[1])
            if 0.0 < float(x) < 90.0:
                self.mongoconn.update({'_id': val}, {'$set': {"updatelat": True, 'glat': x, 'glng': y}})


    def populate_tasks(self):
        # TODO: 提取配置文件
        self.mongoconn = get_mongodb('raw_baidu', "BaiduScene", 'mongo-raw')
        self.to_do_list = list(self.mongoconn.find({'updatelat': False}, {"_id": 1, 'ext.map_x': 1, 'ext.map_y': 1}))
        pages = len(self.to_do_list) / 20 + 1
        self.logger.info('total page: %s' % pages)
        for page in range(pages):
            if self.http_error_count > self.error_limit:
                self.logger.warn('http errors exceed max limit')
                break
            def func(p=page, total=pages):
                res_arr = self.get_latlngs(p, 20)
                idlist = res_arr[0]
                latlngs = res_arr[1]
                self.update_latlngs(idlist, self.baidu_ll_to_google(self.baidu_mc_to_ll(latlngs)))
                self.logger.info('complete: %s / %s' % (p, total))
            self.add_task(func)