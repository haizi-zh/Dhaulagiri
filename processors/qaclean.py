# coding=utf-8
import json
import logging
import re, copy
import time, datetime
import sys
# 手工设置递归调用深度,否则清洗会报RuntimeError: maximum recursion depth exceeded
sys.setrecursionlimit(1000000)

from processors import BaseProcessor
from utils.database import get_mongodb
from gevent.lock import BoundedSemaphore
import lxml.html.soupparser as soupparser
from bs4 import BeautifulSoup

__author__ = 'bxm'


class MafengwoQuProcessor(BaseProcessor):
    name = 'mafengwo-q'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)
        self.build_args()

    def build_args(self):
        """
        处理命令行参数
        """
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument('--limit', type=int)
        parser.add_argument('--skip', type=int, default=0)
        parser.add_argument('--query', type=str)
        self.args, leftover = parser.parse_known_args()

    def populate_tasks(self):
        mafengwo_Qu = get_mongodb('raw_faq', 'MafengwoQuestion', 'mongo-raw')
        # mafengwo_Qu_clean=get_mongodb('raw_faq', 'Question', 'mongo-raw')
        cursor = mafengwo_Qu.find()
        for val in cursor:
            def task(entry=val):
                self.process_detail(entry)

            self.add_task(task)

    def process_detail(self, entry):
        q_id = entry['q_id']
        sourse = {'mafengwo': {'id': q_id}}
        dom = soupparser.fromstring(entry['body'])
        title = dom[0].xpath('//div[@class="q-title"]/h1/text()')[0]
        author = dom[0].xpath('//div[@class="user-bar"]//a[@class="name"]/text()')[0]
        authorId = dom[0].xpath('//div[@class="user-bar"]//a[@class="name"]/@href')[0]
        tmp = re.search(r'\d+', authorId)
        authorId = tmp.group()
        avatar = dom[0].xpath('//div[@class="avatar"]//img/@src')[0]
        # 提取时间，转化为时间戳
        q_time = dom[0].xpath('//span[@class="time"]/text()')[0]
        q_time = '20%s' % q_time
        q_time = time.strptime(q_time, "%Y/%m/%d %H:%M")
        q_time = int(time.mktime(q_time))
        tags = dom[0].xpath('//div[@class="q-tags"]/a[@class="a-tag"]/text()')
        # 查看
        vsCnt = dom[0].xpath('//span[@class="visit"]/text()')[0]
        m = re.search(r'\d+', vsCnt)
        vsCnt = m.group()
        # 收藏
        favorCnt = 0
        # 分享
        shareCnt = 0
        content = self.get_content(entry['body'])
        postType = "question"
        question = {'source': sourse, 'title': title, 'content': content, 'author': author, 'authorId': authorId,
                    'avatar': avatar, 'time': q_time, 'tags': tags, 'vsCnt': vsCnt, 'favorCnt': favorCnt,
                    'shareCnt': shareCnt,
                    'postType': postType}
        mafengwo_Qu_clean = get_mongodb('raw_faq', 'Question', 'mongo-raw')
        mafengwo_Qu_clean.update({'sourse': sourse}, {'$set': question}, upsert=True)

    def get_content(self, body_html):
        """
        将原始数据里body变成清洗后的content
      """
        soup = BeautifulSoup(body_html)
        am = soup.find('div', class_='q-desc')
        content = '<div class="article"></div>'
        Con = BeautifulSoup(content)
        div_tag = Con.div
        for child in am.children:
            if child.name == 'div':
                print child.name
                inner_href = 'http://www.mafengwo.cn%s' % child.a.get('href')
                new_a = Con.new_tag("a", href=inner_href)
                new_a.string = child.a.string
                div_tag.append(new_a)
                new_swap = Con.new_tag("swap")
                new_swap['class'] = "qa_link"
                # swap标签包在a的外面
                new_a.wrap(new_swap)
            else:
                # print child
                newStr = copy.deepcopy(child)
                div_tag.append(newStr)
        return str(Con.div)


class MafengwoAnProcessor(BaseProcessor):
    name = 'mafengwo-a'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)
        self.build_args()

    def build_args(self):
        """
        处理命令行参数
        """
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument('--limit', type=int)
        parser.add_argument('--skip', type=int, default=0)
        parser.add_argument('--query', type=str)
        self.args, leftover = parser.parse_known_args()

    def populate_tasks(self):
        mafengwo_An = get_mongodb('raw_faq', 'MafengwoAnswer', 'mongo-raw')
        cursor = mafengwo_An.find()
        for val in cursor:
            def task(entry=val):
                self.process_detail(entry)

            self.add_task(task)

    def process_detail(self, entry):
        q_id = entry['q_id']
        a_id = entry['a_id']
        sourse = {'mafengwo': {'id': a_id}}
        dom = soupparser.fromstring(entry['body'])
        author = dom[0].xpath('//div[@class="user-bar"]/a[@class="name"]/text()')[0]
        authorId = dom[0].xpath('//div[@class="avatar _j_user_card"]/@data-uid')[0]
        avatar = dom[0].xpath('//div[@class="avatar _j_user_card"]//img[@class="_j_filter_click"]/@src')[0]
        # 提取时间，转化为时间戳
        q_time = dom[0].xpath('//span[@class="time"]/text()')[0]
        match_num = re.findall(r'\d+', q_time)
        if len(match_num) <= 1:
            q_time = int(time.time())
        else:
            q_time = time.strptime(q_time, "%Y-%m-%d %H:%M")
            q_time = int(time.mktime(q_time))
        essence = False
        if entry.has_key('rec'):
            essence = True
        # 得到parentId
        Qu = get_mongodb('raw_faq', 'Question', 'mongo-raw')
        cursor = Qu.find({'sourse': {'mafengwo': {'id': q_id}}})
        parentId = None
        if cursor:
            for val in cursor:
                parentId = val['_id']
                # 评论
        favorCnt = dom[0].xpath('//a[@class="btn-zan"]/span/text()')[0]
        shareCnt = 0
        commentCnt = 0
        if dom[0].xpath('//span[@class="_j_answer_cnum"]/text()'):
            commentCnt = dom[0].xpath('//span[@class="_j_answer_cnum"]/text()')[0]
        content = self.get_content(entry['body'])
        postType = "question"
        answer = {'source': sourse, 'content': content, 'author': author, 'authorId': authorId,
                  'avatar': avatar, 'time': q_time, 'commentCnt': commentCnt, 'favorCnt': favorCnt,
                  'shareCnt': shareCnt,
                  'postType': postType}
        mafengwo_An_clean = get_mongodb('raw_faq', 'Answer', 'mongo-raw')
        mafengwo_An_clean.update({'sourse': sourse}, {'$set': answer}, upsert=True)

    def get_content(self, body_html):
        """
        将原始数据里body变成清洗后的content
      """
        soup = BeautifulSoup(body_html)
        am = soup.find('dd', class_='_j_answer_html')
        content = '<div class="article"></div>'
        Con = BeautifulSoup(content)
        div_tag = Con.div
        for child in am.children:
            if child.name == 'div':
                print child.name
                inner_href = 'http://www.mafengwo.cn%s' % child.a.get('href')
                new_a = Con.new_tag("a", href=inner_href)
                new_a.string = child.a.string
                div_tag.append(new_a)
                new_swap = Con.new_tag("swap")
                new_swap['class'] = "qa_link"
                # swap标签包在a的外面
                new_a.wrap(new_swap)
            else:
                # print child
                newStr = copy.deepcopy(child)
                div_tag.append(newStr)
        return str(Con.div)


class QunarQuProcessor(BaseProcessor):
    name = 'qunar-q'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)
        self.build_args()

    def build_args(self):
        """
        处理命令行参数
        """
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument('--limit', type=int)
        parser.add_argument('--skip', type=int, default=0)
        parser.add_argument('--query', type=str)
        self.args, leftover = parser.parse_known_args()

    def populate_tasks(self):
        qunar_Qu = get_mongodb('raw_faq', 'QunarQuestion', 'mongo-raw')
        # mafengwo_Qu_clean=get_mongodb('raw_faq', 'Question', 'mongo-raw')
        cursor = qunar_Qu.find()
        for val in cursor:
            def task(entry=val):
                self.process_detail(entry)

            self.add_task(task)

    def process_detail(self, entry):
        q_id = entry['q_id']
        sourse = {'qunar': {'id': q_id}}
        title_tmp = BeautifulSoup(entry['title'])
        title = title_tmp.get_text()
        dom = soupparser.fromstring(entry['body'])
        author = dom[0].xpath('//div[@class="authi"]/a/@title')[0]
        authorId = dom[0].xpath('//div[@class="authi"]/a/@href')[0]
        tmp = re.search(r'\d+', authorId)
        authorId = tmp.group()
        avatar = dom[0].xpath('//a[@class="avtm"]/img/@src')[0]
        # 提取时间，转化为时间戳
        q_time = dom[0].xpath('//div[@class="authi"]/em/span/@title')[0]
        q_time = time.strptime(q_time, "%Y-%m-%d %H:%M:%S")
        q_time = int(time.mktime(q_time))
        tags = dom[0].xpath('//div[@class="ptg mbm mtn"]/a/@title')
        # 查看
        vsCnt = 0
        # 收藏
        favorCnt = dom[0].xpath('//span[@id="favoritenumber"]/text()')
        # 分享
        shareCnt = 0
        content = self.get_content(entry['body'])
        postType = "question"
        question = {'source': sourse, 'title': title, 'content': content, 'author': author, 'authorId': authorId,
                    'avatar': avatar, 'time': q_time, 'tags': tags, 'vsCnt': vsCnt, 'favorCnt': favorCnt,
                    'shareCnt': shareCnt,
                    'postType': postType}
        mafengwo_Qu_clean = get_mongodb('raw_faq', 'Question', 'mongo-raw')
        mafengwo_Qu_clean.update({'sourse': sourse}, {'$set': question}, upsert=True)

    def get_content(self, body_html):
        """
        将原始数据里body变成清洗后的content
      """
        soup = BeautifulSoup(body_html)
        am = soup.find('div', class_='q-desc')
        content = '<div class="article"></div>'
        Con = BeautifulSoup(content)
        div_tag = Con.div
        for child in am.children:
            if child.name == 'div':
                print child.name
                inner_href = 'http://www.mafengwo.cn%s' % child.a.get('href')
                new_a = Con.new_tag("a", href=inner_href)
                new_a.string = child.a.string
                div_tag.append(new_a)
                new_swap = Con.new_tag("swap")
                new_swap['class'] = "qa_link"
                # swap标签包在a的外面
                new_a.wrap(new_swap)
            else:
                # print child
                newStr = copy.deepcopy(child)
                div_tag.append(newStr)
        return str(Con.div)






















