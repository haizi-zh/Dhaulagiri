# coding=utf-8

from processors import BaseProcessor
import datetime
import hashlib
import requests
from lxml import etree
from utils.database import get_mongodb
from bson.objectid import ObjectId
from lycity import LyCity
from copy import deepcopy

class OnlineCity(object):
    def __init__(self):
        self.file_url = '/root/Dhaulagiri/tempfile/online_city_final.txt'

    def city_id(self):
        with open(self.file_url, 'r') as f:
            citylist = []
            for line in f:
                city = {}
                city['id'] = line.split(' ')[0]
                city['name'] = line.split(' ')[1]
                citylist.append(city)
            return citylist

    def vs_generate(self):
        citylist = self.city_id()
        for city in citylist:
            id = city['id']
            print city['name']
            query = {'taoziEna': True, 'locality._id': ObjectId(id)}
            col = get_mongodb('poi', 'ViewSpot', 'mongo')
            cousor = col.find(query, {'_id': 1})
            print col.find(query, {'_id': 1}).count()
            for entry in cousor:
                yield entry['_id']


class Ly(LyCity, BaseProcessor):
    name = "tc-api"

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)
        self.version = "20111128102912"  # 服务版本号,长期不变
        self.url = "http://tcopenapi.17usoft.com/handlers/scenery/queryhandler.ashx"  # 正式接口，非测试接口
        self.accountId = "7d9cfec6-0175-419e-9943-d546ff73dec0"  # 账号
        self.accountKey = "fd2c241e5282781b"  # 密码
        self.serviceName = ""  # 接口名字
        self.reqTime = ""  # 时间戳 2014-04-09 09:55:07.020

    def scenerylist(self):
        self.serviceName = "GetSceneryList"
        return self

    def scenerydetail(self):
        self.serviceName = "GetSceneryDetail"
        return self

    def scenerytrafficinfo(self):
        self.serviceName = "GetSceneryTrafficInfo"
        return self

    def sceneryimagelist(self):
        self.serviceName = "GetSceneryImageList"
        return self

    def nearbyscenery(self):
        self.serviceName = "GetNearbyScenery"
        return self

    def sceneryprice(self):
        self.serviceName = "GetSceneryPrice"
        return self

    def pricecalendar(self):
        self.serviceName = "GetPriceCalendar"
        return self

    def get_digitalSign(self):
        version = 'Version=' + self.version
        serviceName = 'ServiceName=' + self.serviceName
        reqTime = 'ReqTime=' + self.reqTime
        accountId = 'AccountID=' + self.accountId
        sorted_array = self.bubbleSort([accountId, reqTime, serviceName, version])
        m = hashlib.md5()
        m.update('&'.join(sorted_array) + self.accountKey)
        return m.hexdigest()


    def bubbleSort(self, origin_array):
        return origin_array


    def update_req_time(self):
        self.reqTime = (datetime.datetime.utcnow() + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S.000')


    def send_request(self, obj_querys):
        res = requests.post(self.url, headers={'Content-Type': 'text/xml'}, data=self.assemble_req_xml(obj_querys))
        self.serviceName = ""
        return res.content


    def assemble_xml_header(self):
        self.update_req_time()
        req_header = ''
        req_header = req_header + '<header>'
        req_header = req_header + '<version>' + self.version + '</version>'
        req_header = req_header + '<accountID>' + self.accountId + '</accountID>'
        req_header = req_header + '<serviceName>' + self.serviceName + '</serviceName>'
        req_header = req_header + '<digitalSign>' + self.get_digitalSign() + '</digitalSign>'
        req_header = req_header + '<reqTime>' + self.reqTime + '</reqTime>'
        req_header = req_header + '</header>'
        return req_header


    def assemble_xml_body(self, query_obj):
        req_body = ''
        req_body = req_body + '<body>'
        for key, value in query_obj.items():
            req_body = req_body + '<' + key + '>' + str(value) + '</' + key + '>'
        req_body = req_body + '</body>'
        return req_body


    def assemble_req_xml(self, query_obj):
        req_xml = ''
        req_xml = req_xml + "<?xml version='1.0' encoding='utf-8' standalone='yes'?>"
        req_xml = req_xml + '<request>'
        req_xml = req_xml + self.assemble_xml_header()
        req_xml = req_xml + self.assemble_xml_body(query_obj)
        req_xml = req_xml + '</request>'
        return req_xml


    def match_by_name(self):
        """
        match viewspot by extract name
        :return:
        """
        con_lymap = get_mongodb('poi', 'LyMapping', 'mongo')
        con_locality = get_mongodb('geo', 'Locality', 'mongo')
        con_vs = get_mongodb('poi', 'ViewSpot', 'mongo')

        for viewspot_id in OnlineCity().vs_generate():
            def func(vs_id=viewspot_id):
                doc = con_vs.find_one({'_id': ObjectId(vs_id)}, {'locality.zhName': 1, 'zhName': 1})
                local_name = doc['locality']['zhName'].encode('utf-8')
                vs_name = doc['zhName'].encode('utf-8')
                ly_doc = con_locality.find_one({'alias': local_name}, {'source.ly.id': 1})
                if ly_doc == None:
                    return
                try:
                    ly_city_id = ly_doc['source']['ly']['id']
                except KeyError:
                    # print 'error'
                    return

                if ly_city_id < 36:
                    raw_xml = self.scenerylist().send_request({'keyword': vs_name, 'searchFields': "sceneryName", 'clientIp' : '127.0.0.1', 'provinceId': ly_city_id})
                else:
                    raw_xml = self.scenerylist().send_request({'keyword': vs_name, 'searchFields': "sceneryName", 'clientIp' : '127.0.0.1', 'cityId': ly_city_id})

                node = etree.fromstring(raw_xml)
                responce_code = node.xpath('//rspCode/text()')[0]
                if '0000' == str(responce_code):
                    vs_nodes = node.xpath('//sceneryName')
                    for vs in vs_nodes:
                        name = vs.text.encode('utf-8')
                        ly_id = vs.xpath('../sceneryId')[0].text
                        if name == vs_name:
                            print name
                            con_lymap.update({'itemId': ObjectId(vs_id)}, {'$set': {'itemId': ObjectId(vs_id), 'zhNameLxp': vs_name, 'locationName': local_name, 'lyCityId': ly_city_id, 'zhNameLy': name, 'lyId': ly_id}}, upsert=True)
                            # con_lymap.save({'itemId': ObjectId(vs_id), 'zhNameLxp': vs_name, 'locationName': local_name, 'lyCityId': ly_city_id, 'zhNameLy': name, 'lyId': ly_id})
            self.add_task(func)


    def crawl_vs(self):
        """
        crawl vs with ly api through city iteration
        :return:
        """
        conn = get_mongodb('raw_ly', 'ViewSpot', 'mongo-raw')
        city_list = self.crawl_city()
        self.logger.info('-=-=-=-=length: %s' % len(city_list))
        for ct in city_list:
            def func(city=ct):
                self.logger.info('================%s==============' % city)
                if int(city['location_id']) <= 35:
                    query_obj = {'clientIp': '127.0.0.1', 'provinceId': int(city['location_id'])}
                elif int(city['location_id']) <= 404:
                    query_obj = {'clientIp': '127.0.0.1', 'cityId': int(city['location_id'])}
                else:
                    query_obj = {'clientIp': '127.0.0.1', 'countryId': int(city['location_id'])}
                raw_xml = self.scenerylist().send_request(query_obj)
                node = etree.fromstring(raw_xml)
                responce_code = node.xpath('//rspCode/text()')[0]
                if '0000' == str(responce_code):
                    total_page = node.xpath('//sceneryList')[0].attrib['totalPage']
                    for page in xrange(int(total_page)):
                        temp_query = deepcopy(query_obj)
                        temp_query['page'] = page + 1
                        raw_xml = self.scenerylist().send_request(temp_query)
                        node = etree.fromstring(raw_xml)
                        vs_nodes = node.xpath('//sceneryName')
                        for vs in vs_nodes:
                            name = vs.text.encode('utf-8')
                            ly_id = int(vs.xpath('../sceneryId')[0].text)
                            self.logger.info('----%s-----%s' % (name, ly_id))
                            conn.update({'lyId': ly_id}, {'$set': {'lyId': ly_id, 'lyName': name}}, upsert=True)
            self.add_task(func)



    def populate_tasks(self):
        self.crawl_vs()
