# coding=utf-8

from processors import BaseProcessor
from utils.database import get_mongodb

from lxml import etree

class LyCity(BaseProcessor):

    name = 'crawl_tc_city'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)

    def url_generate(self):
        for i in xrange(34):
            yield 'http://www.ly.com/scenery/scenerysearchlist_' + str(2+i) + '_0__0_0_0_0_0_0.html'


    def rename_citys(self, province_name):
        if province_name == u'澳门澳门特别行政区':
            province_name = u'澳门'
        return province_name

    def crawl_city(self, *args, **kwargs):
        location_list = []
        for url in self.url_generate():
            temp = {}
            res = self.request.get(url)
            res_html = res.content
            title = etree.fromstring(res_html, etree.HTMLParser()).xpath("//head[@id='Head1']/title/text()")[0]
            idx = title.index(u'景区')
            assert idx != -1
            province_name = title[ : idx]
            province_id = res.url.split('_')[1]
            temp['location_name'] = self.rename_citys(province_name)
            temp['location_id'] = province_id
            location_list.append(temp)

            city_name = etree.fromstring(res_html, etree.HTMLParser()).xpath("//div[@class='search_screen_dl']//dl[2]//div[@class='right']/a/@title")
            city_id = etree.fromstring(res_html, etree.HTMLParser()).xpath("//div[@class='search_screen_dl']//dl[2]//div[@class='right']/a/@tvalue")
            for name, id in zip(city_name, city_id):
                temp = {}
                temp['location_name'] = self.rename_citys(name)
                temp['location_id'] = id
                location_list.append(temp)
                print id
        self.logger.info(location_list)
        return location_list


    def match_ly_city(self):
        """
        match ly city to lxp Locality by alias field
        :return:
        """
        location = self.crawl_city()
        dbcon = get_mongodb('geo', 'Locality', 'mongo')
        for ele in location:
            count = dbcon.find({'alias': ele['location_name']}).count()
            if count == 1:
                dbcon.update({'alias': ele['location_name']}, {'$set': {'source.ly': {'id': int(ele['location_id'])}}})
            elif count > 1:
                self.logger.info('---More-Than-One: %s --- %s' % (ele['location_name'], ele['location_id']))
            else:
                self.logger.info('---Not-Found: %s --- %s' % (ele['location_name'], ele['location_id']))


    def populate_tasks(self):
        self.match_ly_city()
