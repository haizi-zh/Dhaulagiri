# coding=utf-8
import argparse
import logging
import re
from hashlib import md5

import gevent
import pymongo
from pymongo.errors import DuplicateKeyError
import requests
import lxml.html.soupparser as soupparser

from processors import BaseProcessor
from utils.database import get_mongodb


__author__ = 'wdx'

class CtripQuestionClean(BaseProcessor):
    name = "ctrip_question"

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)
        self.args = self.args_builder()


    def args_builder(self):
        parser = self.arg_parser
        parser.add_argument('--limit', type=int)
        parser.add_argument('--skip', default=0, type=int)
        parser.add_argument('--url-filter', type=str)
        parser.add_argument('--query', type=str)
        parser.add_argument('--fetch', action='store_true')
        args, leftover = parser.parse_known_args()
        return args

    def populate_tasks(self):
        cq = get_mongodb('raw_faq', 'CtripQuestion', 'mongo-raw')
        cq_c = get_mongodb('raw_faq', 'CtripClean', 'mongo-raw')
        cq_cursor = cq.find()

        for cq_list in cq_cursor:

            import time
            def task(val = cq_list):
                tags = []
                q_id = int(val['q_id'])
                question = {}
                dom = soupparser.fromstring(val['body'])
                username = dom[0].xpath('//a[@class = "ask_username"]/text()')[0]
                publictime = dom[0].xpath('//span[@class = "ask_time"]/text()')[0]
                publictime = publictime[3:]
                if len(publictime) > 7:
                    publictime = time.mktime(time.strptime(publictime,'%Y-%m-%d %H:%M:%S'))
                else:
                    publictime = time.time()

                title = dom[0].xpath('//h1[@class = "ask_title"]/text()')[1]
                content = dom[0].xpath('//p[@class = "ask_text"]//text()')[0]
                tag = dom[0].xpath('//div[@class = "asktag_oneline cf"]//text()')
                for t in tag:
                    if len(t) > 1:
                        tags.append(t)

                postType =  "question"


                question = {'q_id' : q_id,'source':'ctrip','source_id':q_id,'title' :title,'author' : username,'time': publictime,'content' : content,
                            'tags':tags, 'avatar': None,'authorId':None, 'essence':None ,'favorCnt':None,
                            'postType':postType}
                cq_c.update({'q_id':q_id},{'$set': question},upsert=True)

            self.add_task(task)

class CtripAnswerClean(BaseProcessor):
    name = "ctrip_answer"

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)
        self.args = self.args_builder()


    def args_builder(self):
        parser = self.arg_parser
        parser.add_argument('--limit', type=int)
        parser.add_argument('--skip', default=0, type=int)
        parser.add_argument('--url-filter', type=str)
        parser.add_argument('--query', type=str)
        parser.add_argument('--fetch', action='store_true')
        args, leftover = parser.parse_known_args()
        return args

    def populate_tasks(self):
        ca = get_mongodb('raw_faq', 'CtripAnswer' , 'mongo-raw')
        ca_c = get_mongodb('raw_faq', 'CtripClean', 'mongo-raw')
        ca_cursor = ca.find()

        for ca_list in ca_cursor:

            import time
            def task(val = ca_list):
                q_id = int(val['q_id'])
                dom = soupparser.fromstring(val['body'])

                username = dom[0].xpath('//a[@class = "answer_id"]/text()')[0]
                userid = dom[0].xpath('//div[@data-answeruserid]/@data-answeruserid')[0]
                publictime = dom[0].xpath('//span[@class = "answer_time"]/text()')[0]
                publictime = publictime[3:]
                if len(publictime) > 7:
                    publictime = time.mktime(time.strptime(publictime,'%Y-%m-%d %H:%M:%S'))
                else:
                    publictime = time.time()

                title = None
                content = dom[0].xpath('//p[@class = "answer_text"]//text()')[0]

                postType =  "answer"
                tmp = dom[0].xpath('//h2[@class = "bestanswer_title"]')
                if tmp:
                    essence = True
                else:
                    essence = False

                tmp = dom[0].xpath('//a[@class = "btn_answer_zan"]/span/text()')
                if tmp:
                    favorCnt = tmp[0]
                else:
                    favorCnt = 0
                avatar = 'http://you.ctrip.com' + dom[0].xpath('//a[@class = "answer_img"]/@href')[0]

                answer = {'q_id' : q_id,'source':'ctrip','source_id':val['a_id'],'title' :title,'author' : username,'time': publictime,'content' : content,
                            'tags':None, 'avatar': avatar,'authorId':userid, 'essence':essence ,'favorCnt':favorCnt,
                            'postType':postType}
                ca_c.update({'source_id':answer['source_id']},{'$set':answer},upsert =True)

            self.add_task(task)







