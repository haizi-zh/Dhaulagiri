# coding=utf-8

import json
import logging
import re

from processors import BaseProcessor
from utils.database import get_mongodb


class Restaurants(BaseProcessor):

    name = 'restaurants'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)

        self.target_cities = []
        self.req_url = ''
        self.post_data = None


    def populate_tasks(self):
        print '=== begin ==='

        target_cities = ['东京', '京都', '大阪', '札幌', '小樽', '富良野', '登别', '函管', '旭川',
                            '首尔', '釜山', '济州岛']
        req_url = 'http://182.92.222.231:8887/restaurant/search.json'

        col_restaurant = get_mongodb('raw_misc', 'Tongue', 'mongo-raw')

        error_list = []

        def set_post_data(city="", page_num=10, skip=0):
            """
            设置post数据
            :param city:
            :param page_num:
            :param skip:
            :return:
            """
            post_data = {
                "business": 0,
                "price_high": 0,
                "nToSkip": skip,  # ++
                "kind_lable": [],
                "terminal": "ios",
                "key": "",
                "sort_key": "normal",
                "nToReturn": page_num,  # Available Max: 10
                "region": city,
                "price_low": 0,
                "distance": -1,
                "score": 0,
                "locality": ""
            }
            return post_data

        def send_post_request(data, url=req_url):
                    """
                    发送http post请求
                    :param data:
                    :param url:
                    :return:
                    """
                    try:
                        response = self.request.post(url, data=json.dumps(data))
                    except IOError:
                        self.logger.warn('IOError: page %d in %s' % (data['nToSkip'], data['region']))
                        error_list.append(data)
                        return

                    if not response or response.json()['error_code'] != '0':
                        self.logger.warn('No response: page %d in %s' % (data['nToSkip'], data['region']))
                        error_list.append(data)
                        return

                    return response.json()

        for city in target_cities:
            page_index = 0
            page_num = 10

            post_data = set_post_data(city=city, page_num=page_num, skip=page_index)
            respond_data = send_post_request(post_data)

            if not respond_data:
                continue

            page_count = respond_data['count']
            entry_flag = 0
            # reset error_list
            error_list = []

            if page_count < page_num:
                print 'just one page over'
            else:
                while page_num * page_index < page_count:
                    page_index += 1
                    post_data = set_post_data(city=city, page_num=page_num, skip=page_index)
                    respond_data = send_post_request(post_data)
                    entrys = respond_data["list"]
                    print len(entrys)
                    for entry in entrys:
                        col_restaurant.update({'id': entry['id']}, {'$set': entry}, upsert=True)
                        entry_flag += 1
                        print entry_flag

            # process error pages
            while len(error_list) != 0:
                for error_page in error_list:
                    respond_data = send_post_request(error_page)
                    if respond_data:
                        error_list.remove(error_page)
                        entry_flag += 1
                        print entry_flag

            if entry_flag == page_count:
                self.logger.warn('Successful: %s', city)