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

        cursor = mafengwo_Qu.find()
        for val in cursor:
            def task(entry=val):
                self.process_detail(entry)

            self.add_task(task)

    def process_detail(self, entry):
        q_id = entry['q_id']
        source = {'mafengwo': {'id': q_id}}
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

        question = {'title': title, 'content': content, 'author': author, 'authorId': authorId, 'avatar': avatar,
                    'time': q_time, 'tags': tags, 'vsCnt': vsCnt, 'favorCnt': favorCnt, 'shareCnt': shareCnt,
                    'postType': postType, 'quote': None}
        mafengwo_Qu_clean = get_mongodb('raw_faq', 'Question', 'mongo-raw')
        mafengwo_Qu_clean.update({'source': source}, {'$set': question}, upsert=True)

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
                inner_href = 'http://www.mafengwo.cn%s' % child.a.get('href')
                new_a = Con.new_tag("a", href=inner_href)
                new_a.string = child.a.string
                div_tag.append(new_a)
                new_span = Con.new_tag("span")
                new_span['class'] = "qa_link"
                # swap标签包在a的外面
                new_a.wrap(new_span)
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
        source = {'mafengwo': {'id': a_id}}
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
        cursor = Qu.find({'source': {'mafengwo': {'id': q_id}}})
        parentId = None
        if cursor:
            for val in cursor:
                parentId = val['_id']

        favorCnt = dom[0].xpath('//a[@class="btn-zan"]/span/text()')[0]
        shareCnt = 0
        commentCnt = 0
        if dom[0].xpath('//span[@class="_j_answer_cnum"]/text()'):
            commentCnt = dom[0].xpath('//span[@class="_j_answer_cnum"]/text()')[0]
        content = self.get_content(entry['body'])
        postType = "answer"
        answer = {'content': content, 'author': author, 'authorId': authorId, 'avatar': avatar, 'time': q_time,
                  'commentCnt': commentCnt, 'favorCnt': favorCnt, 'shareCnt': shareCnt, 'postType': postType,
                  'essence': essence, 'parentId': parentId, 'quote': None}
        mafengwo_An_clean = get_mongodb('raw_faq', 'Answer', 'mongo-raw')
        mafengwo_An_clean.update({'source': source}, {'$set': answer}, upsert=True)

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
                inner_href = 'http://www.mafengwo.cn%s' % child.a.get('href')
                new_a = Con.new_tag("a", href=inner_href)
                new_a.string = child.a.string
                div_tag.append(new_a)
                new_span = Con.new_tag("span")
                new_span['class'] = "qa_link"
                # swap标签包在a的外面
                new_a.wrap(new_span)
            else:
                newStr = copy.deepcopy(child)
                div_tag.append(newStr)
        return str(Con.div)


class CtripQuProcessor(BaseProcessor):
    name = 'ctrip-q'

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
        ctrip_Qu = get_mongodb('raw_faq', 'CtripQuestion', 'mongo-raw')
        cursor = ctrip_Qu.find()
        for val in cursor:
            def task(entry=val):
                self.process_detail(entry)

            self.add_task(task)

    def process_detail(self, entry):
        q_id = entry['q_id']
        source = {'ctrip': {'id': q_id}}
        dom = soupparser.fromstring(entry['body'])
        title = dom[0].xpath('//h1[@class="ask_title"]//text()')[1]
        author = dom[0].xpath('//a[@class="ask_username"]/text()')[0]
        authorId = ""
        avatar = ""
        # 提取时间，转化为时间戳
        q_time = dom[0].xpath('//span[@class="ask_time"]/text()')[0]
        if len(re.findall(r'\d+', q_time)) <= 1:
            q_time = time.time()
        else:
            m = re.search(r'[0-9-:\s]+', q_time)
            q_time = m.group()
            q_time = time.strptime(q_time.strip(), "%Y-%m-%d %H:%M:%S")
            q_time = int(time.mktime(q_time))
        tags = dom[0].xpath('//div[@class="asktag_oneline cf"]/a[@class="asktag_item"]/@title')
        # 查看
        vsCnt = 0
        # 收藏
        favorTmp = dom[0].xpath('//a[@class="link_share"]/span/text()')
        favorCnt = 0 if not favorTmp else favorTmp[0]
        # 分享
        shareTmp = dom[0].xpath('//a[@class="link_share"]/span/text()')
        shareCnt = 0 if not favorTmp else shareTmp[0]
        content = self.get_content(entry['body'])
        postType = "question"
        question = {'title': title, 'content': content, 'author': author, 'authorId': authorId,
                    'avatar': avatar, 'time': q_time, 'tags': tags, 'vsCnt': vsCnt, 'favorCnt': favorCnt,
                    'shareCnt': shareCnt, 'postType': postType, 'quote': None}
        ctrip_Qu_clean = get_mongodb('raw_faq', 'Question', 'mongo-raw')
        ctrip_Qu_clean.update({'source': source}, {'$set': question}, upsert=True)

    def get_content(self, body_html):
        """
          将原始数据里body变成清洗后的content
        """
        soup = BeautifulSoup(body_html)
        ask_text = soup.find('p', class_='ask_text')
        content = '<div class="article"></div>'
        Con = BeautifulSoup(content)
        div_tag = Con.div
        for child in ask_text.children:
            # 如果是链接的话
            if child.name == "a":
                inner_href = child.get('href')
                new_a = Con.new_tag("a", href=inner_href)
                new_a.string = child.string
                div_tag.append(new_a)
                new_span = Con.new_tag("span")
                new_span['class'] = "qa_link"
                # swap标签包在a的外面
                new_a.wrap(new_span)
            # 如果是文字或者</br>的话的话
            else:
                newStr = copy.deepcopy(child)
                div_tag.append(newStr)
        # 图片列表
        ask_piclist = soup.find('div', class_='ask_piclist cf')
        if ask_piclist:
            # 将图片加入content
            for child in ask_piclist:
                if child.name == "a":
                    pic_href = child.get('href')
                    img_id = child.get('data-img-id')
                    new_div = Con.new_tag("div", id=img_id)
                    new_div['class'] = 'zoom'
                    new_img = Con.new_tag("img", href=pic_href)
                    new_div.append(new_img)
                    div_tag.append(new_div)
        return str(div_tag)


class CtripAnProcessor(BaseProcessor):
    name = 'ctrip-a'

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
        ctrip_An = get_mongodb('raw_faq', 'CtripAnswer', 'mongo-raw')
        cursor = ctrip_An.find()
        for val in cursor:
            def task(entry=val):
                self.process_detail(entry)

            self.add_task(task)

    def process_detail(self, entry):
        q_id = entry['q_id']
        a_id = entry['a_id']
        source = {'ctrip': {'id': a_id}}
        dom = soupparser.fromstring(entry['body'])
        author = dom[0].xpath('//p[@class="answer_user"]/a[@class="answer_id"]/text()')[0]
        authorId = ""
        avatar = dom[0].xpath('//a[@class="answer_img"]/img/@src')[0]
        # 提取时间，转化为时间戳
        q_time = dom[0].xpath('//span[@class="answer_time"]/text()')[0]
        if len(re.findall(r'\d+', q_time)) <= 1:
            q_time = int(time.time())
        else:
            m = re.search(r'[0-9-:\s]+', q_time)
            q_time = m.group()
            q_time = time.strptime(q_time.strip(), "%Y-%m-%d %H:%M:%S")
            q_time = int(time.mktime(q_time))
        essence = False
        if entry.has_key('rec'):
            essence = True
        # 得到parentId
        Qu = get_mongodb('raw_faq', 'Question', 'mongo-raw')
        cursor = Qu.find({'source': {'ctrip': {'id': q_id}}})
        parentId = None
        if cursor:
            for val in cursor:
                parentId = val['_id']
        favorTmp = dom[0].xpath('//a[@class="btn_answer_zan"]/span/text()')
        favorCnt = 0 if not favorTmp else favorTmp[0]
        shareCnt = 0
        comments = dom[0].xpath('//ul[@class="answer_comment_list"]/li')
        commentCnt = len(comments)
        content = self.get_content(entry['body'])
        postType = "answer"
        answer = {'content': content, 'author': author, 'authorId': authorId, 'avatar': avatar, 'time': q_time,
                  'commentCnt': commentCnt, 'favorCnt': favorCnt, 'shareCnt': shareCnt, 'postType': postType,
                  'essence': essence, 'parentId': parentId, 'quote': None}
        mafengwo_An_clean = get_mongodb('raw_faq', 'Answer', 'mongo-raw')
        mafengwo_An_clean.update({'source': source}, {'$set': answer}, upsert=True)

    def get_content(self, body_html):
        """
        将原始数据里body变成清洗后的content
      """
        soup = BeautifulSoup(body_html)
        ask_text = soup.find('p', class_='answer_text')
        content = '<div class="article"></div>'
        Con = BeautifulSoup(content)
        div_tag = Con.div
        for child in ask_text.children:
            # 如果是链接的话
            if child.name == "a":
                inner_href = child.get('href')
                new_a = Con.new_tag("a", href=inner_href)
                new_a.string = child.string
                div_tag.append(new_a)
                new_span = Con.new_tag("span")
                new_span['class'] = "qa_link"
                # swap标签包在a的外面
                new_a.wrap(new_span)
            # 如果是文字或者</br>的话的话
            else:
                newStr = copy.deepcopy(child)
                div_tag.append(newStr)
        # 图片列表
        ask_piclist = soup.find('div', class_='ask_piclist cf')
        if ask_piclist:
            # 将图片加入content
            for child in ask_piclist:
                if child.name == "a":
                    pic_href = child.get('href')
                    img_id = child.get('data-img-id')
                    new_div = Con.new_tag("div", id=img_id)
                    new_div['class'] = 'zoom'
                    new_img = Con.new_tag("img", href=pic_href)
                    new_div.append(new_img)
                    div_tag.append(new_div)
        return str(div_tag)


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
        cursor = qunar_Qu.find()
        for val in cursor:
            def task(entry=val):
                self.process_detail(entry)

            self.add_task(task)

    def process_detail(self, entry):
        q_id = entry['q_id']
        source = {'qunar': {'id': q_id}}
        # 原始数据格式不统一
        if isinstance(entry['title'], list):
            entry['title'] = entry['title'][0]
        title_tmp = BeautifulSoup(entry['title'])
        title = title_tmp.get_text()
        dom = soupparser.fromstring(entry['body'])
        author = dom[0].xpath('//div[@class="authi"]/a/@title')[0]
        authorHref = dom[0].xpath('//div[@class="authi"]/a/@href')[0]
        authorId = re.search(r'\d+', authorHref).group()
        avatar = dom[0].xpath('//a[@class="avtm"]/img/@src')[0]
        # 提取时间，转化为时间戳
        time_tmp = dom[0].xpath('//div[@class="authi"]/em/span/@title')
        if time_tmp:
            q_time = time_tmp[0]
        else:
            q_time = dom[0].xpath('//div[@class="authi"]/em/text()')[0]
            m = re.search(r'[0-9-:\s]+', q_time)
            q_time = m.group()
        q_time = time.strptime(q_time.strip(), "%Y-%m-%d %H:%M:%S")
        q_time = int(time.mktime(q_time))
        tags = dom[0].xpath('//div[@class="ptg mbm mtn"]/a/text()')
        # 查看
        vsCnt = 0
        # 收藏
        favorCnt = dom[0].xpath('//span[@id="favoritenumber"]/text()')[0]
        # 分享
        shareCnt = 0
        content_quote = self.get_content_quote(entry['body'])
        content = content_quote['content']
        quote = content_quote['quote']
        postType = "question"
        question = {'title': title, 'content': content, 'quote': quote, 'author': author, 'authorId': authorId,
                    'avatar': avatar, 'time': q_time, 'tags': tags, 'vsCnt': vsCnt, 'favorCnt': favorCnt,
                    'shareCnt': shareCnt, 'postType': postType}
        qunar_Qu_clean = get_mongodb('raw_faq', 'Question', 'mongo-raw')
        qunar_Qu_clean.update({'source': source}, {'$set': question}, upsert=True)


    def get_content_quote(self, body_html):
        """
          将原始数据里body变成清洗后的content和quote
        """
        soup = BeautifulSoup(body_html)
        ask_text = soup.find('td', class_='t_f')
        #quote
        quote = '<div class="quote"><blockquote></blockquote></div>'
        Quo = BeautifulSoup(quote)
        blockquote_tag = Quo.blockquote
        #content
        content = '<div class="article"></div>'
        Con = BeautifulSoup(content)
        div_tag = Con.div

        def process_quote(q_quote):

            for child in q_quote.children:
                # 文字
                if not child.name:
                    new_font = Quo.new_tag("font")
                    new_font['class'] = "pre_article"
                    new_font.string = child.string.strip("\r\n")
                    blockquote_tag.append(new_font)

                elif child.name == "a":
                    if len(child.contents) > 1 or not child.get_text():
                        process_quote(child)
                        continue
                    new_a = Quo.new_tag("a", href=child.get('href'))
                    new_a.string = child.string
                    blockquote_tag.append(new_a)
                    new_span = Quo.new_tag("span")
                    new_span['class'] = "pre_link"
                    # span标签包在a的外面
                    new_a.wrap(new_span)

                elif child.name == "img":
                    pic_href = child.get('src')
                    img_id = child.get('id')
                    new_div = Quo.new_tag("div", id=img_id)
                    new_div['class'] = 'pre_zoom'
                    new_img = Quo.new_tag("img", href=pic_href)
                    new_div.append(new_img)
                    blockquote_tag.append(new_div)

                elif child.name == "font":
                    process_quote(child)

                elif child.name == "strong":
                    process_quote(child)

                elif child.name == "blockquote":
                    process_quote(child)

                # </br>
                else:
                    new_str = copy.deepcopy(child)
                    blockquote_tag.append(new_str)

        def process_content(q_article):

            for child in q_article.children:
                # 如果是文字的话
                if not child.name:
                    new_str = copy.deepcopy(child)
                    div_tag.append(new_str.strip("\r\n"))

                # 如果是链接的话
                elif child.name == "a":
                    if len(child.contents) > 1 or not child.get_text():
                        process_content(child)
                        continue
                    inner_href = child.get('href')
                    new_a = Con.new_tag("a", href=inner_href)
                    new_a.string = child.string
                    div_tag.append(new_a)
                    new_span = Con.new_tag("span")
                    new_span['class'] = "qa_link"
                    # span标签包在a的外面
                    new_a.wrap(new_span)

                elif child.name == "font":
                    process_content(child)

                elif child.name == "strong":
                    process_content(child)

                # 如果是引用的话
                elif child.name == "blockquote":
                    process_quote(child)

                # 如果是图片的话
                elif child.name == "ignore_js_op":
                    pic_href = child.div.img.get('src')
                    pic_href = "http://travel.qunar.com/bbs/%s" % pic_href
                    img_id = child.div.img.get('id')
                    new_div = Con.new_tag("div", id=img_id)
                    new_div['class'] = 'qa_zoom'
                    new_img = Con.new_tag("img", href=pic_href)
                    new_div.append(new_img)
                    div_tag.append(new_div)

                elif child.name == "img":
                    pic_href = child.get('src')
                    img_id = child.get('id')
                    new_div = Con.new_tag("div", id=img_id)
                    new_div['class'] = 'qa_zoom'
                    new_img = Con.new_tag("img", href=pic_href)
                    new_div.append(new_img)
                    div_tag.append(new_div)

                elif child.name == "div":
                    #如果是引用的话
                    if child.get('class') == "quote":
                        process_quote(child)
                    else:
                        process_content(child)

                # 如果是</br>的话
                elif child.name == "br":
                    new_br = Con.new_tag("br")
                    div_tag.append(new_br)


        process_content(ask_text)
        # 附件中图片列表
        ask_piclist = None if not soup.find('div', class_='pattl') else soup.find('div', class_='pattl')
        if ask_piclist and ask_piclist.find('img'):
            # 将图片加入content
            for child in ask_piclist.find_all("div", "mbn savephotop"):
                pic_href = child.img.get('src')
                pic_href = "http://travel.qunar.com/bbs/%s" % pic_href
                img_id = child.img.get('id')
                new_div = Con.new_tag("div", id=img_id)
                new_div['class'] = 'qa_zoom'
                new_img = Con.new_tag("img", href=pic_href)
                new_div.append(new_img)
                div_tag.append(new_div)

        content = str(Con.div)
        # 如果quote里没有内容，将其置为None
        quote = str(Quo.div) if blockquote_tag.contents else None
        return {"content": content, "quote": quote}


class QunarAnProcessor(BaseProcessor):
    name = 'qunar-a'

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
        qunar_An = get_mongodb('raw_faq', 'QunarAnswer', 'mongo-raw')
        cursor = qunar_An.find()
        for val in cursor:
            def task(entry=val):
                self.process_detail(entry)

            self.add_task(task)

    def process_detail(self, entry):
        q_id = entry['q_id']
        a_id = entry['post_id']
        source = {'qunar': {'id': a_id}}
        dom = soupparser.fromstring(entry['body'])
        author = dom[0].xpath('//div[@class="authi"]/a/@title')[0]
        authorHref = dom[0].xpath('//div[@class="authi"]/a/@href')[0]
        tmp = re.search(r'\d+', authorHref)
        authorId = tmp.group()
        avatar = dom[0].xpath('//a[@class="avtm"]/img/@src')[0]
        # 提取时间，转化为时间戳
        time_tmp = dom[0].xpath('//div[@class="authi"]/em/span/@title')
        if time_tmp:
            q_time = time_tmp[0]
        # 因为格式不统一
        else:
            q_time = dom[0].xpath('//div[@class="authi"]/em/text()')[0]
            m = re.search(r'[0-9-:\s]+', q_time)
            q_time = m.group()
        q_time = time.strptime(q_time.strip(), "%Y-%m-%d %H:%M:%S")
        q_time = int(time.mktime(q_time))
        # 收藏
        if dom[0].xpath('//a[@class="replyadd"]/span/text()'):
            favorCnt = dom[0].xpath('//a[@class="replyadd"]/span/text()')[0]
        else:
            favorCnt = 0
        # 分享
        shareCnt = 0
        commentCnt = 0
        essence = False
        # 得到parentId
        Qu = get_mongodb('raw_faq', 'Question', 'mongo-raw')
        cursor = Qu.find({'source': {'qunar': {'id': q_id}}})
        parentId = None
        if cursor:
            for val in cursor:
                parentId = val['_id']
        content_quote = self.get_content_quote(entry['body'], q_id)
        content = content_quote['content']
        quote = content_quote['quote']
        postType = "answer"
        answer = {'content': content, 'author': author, 'authorId': authorId, 'avatar': avatar, 'time': q_time,
                  'commentCnt': commentCnt, 'favorCnt': favorCnt, 'shareCnt': shareCnt, 'postType': postType,
                  'essence': essence, 'parentId': parentId, "quote": quote}
        qunar_Qu_clean = get_mongodb('raw_faq', 'Answer', 'mongo-raw')
        qunar_Qu_clean.update({'source': source}, {'$set': answer}, upsert=True)


    def get_content_quote(self, body_html, q_id):
        """
          将原始数据里body变成清洗后的content和quote
        """
        soup = BeautifulSoup(body_html)
        ask_text = soup.find('td', class_='t_f')

        quote = '<div class="quote"><blockquote></blockquote></div>'
        Quo = BeautifulSoup(quote)
        blockquote_tag = Quo.blockquote

        content = '<div class="article"></div>'
        Con = BeautifulSoup(content)
        div_tag = Con.div

        def process_quote(q_quote):
            for child in q_quote.children:

                # 文字
                if not child.name:
                    new_font = Quo.new_tag("font")
                    new_font['class'] = "pre_article"
                    new_font.string = child.string.strip("\r\n")
                    blockquote_tag.append(new_font)

                elif child.name == "a":
                    if not child.get_text():
                        process_quote(child)
                        continue
                    inner_href = child.get('href')
                    new_a = Quo.new_tag("a", href=inner_href)
                    new_a.string = child.string
                    blockquote_tag.append(new_a)
                    new_span = Quo.new_tag("span")
                    new_span['class'] = "pre_link"
                    # span标签包在a的外面
                    new_a.wrap(new_span)

                elif child.name == "img":
                    pic_href = child.get('src')
                    img_id = child.get('id')
                    new_div = Quo.new_tag("div", id=img_id)
                    new_div['class'] = 'pre_zoom'
                    new_img = Quo.new_tag("img", href=pic_href)
                    new_div.append(new_img)
                    blockquote_tag.append(new_div)

                elif child.name == "blockquote":
                    process_quote(child)

                elif child.name == "font":
                    process_quote(child)

                elif child.name == "strong":
                    process_quote(child)

                # </br>
                else:
                    new_str = copy.deepcopy(child)
                    blockquote_tag.append(new_str)

        def process_content(q_article):

            for child in q_article.children:

                # 如果是文字的话
                if not child.name:
                    new_str = copy.deepcopy(child)
                    div_tag.append(new_str.strip("\r\n"))

                # 如果是链接的话
                elif child.name == "a":
                    if len(child.contents) > 1 or not child.get_text():
                        process_content(child)
                        continue
                    inner_href = child.get('href')
                    new_a = Con.new_tag("a", href=inner_href)
                    new_a.string = child.string
                    div_tag.append(new_a)
                    new_span = Con.new_tag("span")
                    new_span['class'] = "qa_link"
                    # span标签包在a的外面
                    new_a.wrap(new_span)

                elif child.name == "font":
                    process_content(child)

                elif child.name == "strong":
                    process_content(child)

                # 如果是引用的话
                elif child.name == "blockquote":
                    process_quote(child)

                # 如果是图片的话
                elif child.name == "ignore_js_op":
                    pic_href = child.div.img.get('src')
                    pic_href = "http://travel.qunar.com/bbs/%s" % pic_href
                    img_id = child.div.img.get('id')
                    new_div = Con.new_tag("div", id=img_id)
                    new_div['class'] = 'qa_zoom'
                    new_img = Con.new_tag("img", href=pic_href)
                    new_div.append(new_img)
                    div_tag.append(new_div)

                elif child.name == "img":
                    pic_href = child.get('src')
                    if not child.get('id'):
                        continue
                    img_id = child.get('id')
                    new_div = Con.new_tag("div", id=img_id)
                    new_div['class'] = 'qa_zoom'
                    new_img = Con.new_tag("img", href=pic_href)
                    new_div.append(new_img)
                    div_tag.append(new_div)

                elif child.name == "div":
                    #如果是引用的话
                    if child.get('class') == "quote":
                        process_quote(child)
                    else:
                        process_content(child)

                # 如果是</br>的话
                elif child.name == "br":
                    new_br = Con.new_tag("br")
                    div_tag.append(new_br)


        process_content(ask_text)

        # 附件中图片列表
        ask_piclist = None if not soup.find('div', class_='pattl') else soup.find('div', class_='pattl')
        if ask_piclist and ask_piclist.find('img'):
            # 将图片加入content
            for child in ask_piclist.find_all("div", "mbn savephotop"):
                pic_href = child.img.get('src')
                pic_href = "http://travel.qunar.com/bbs/%s" % pic_href
                img_id = child.img.get('id')
                new_div = Con.new_tag("div", id=img_id)
                new_div['class'] = 'qa_zoom'
                new_img = Con.new_tag("img", href=pic_href)
                new_div.append(new_img)
                div_tag.append(new_div)

        content = str(Con.div)
        # 如果quote里没有内容，将其置为None
        quote = str(Quo.div) if blockquote_tag.contents else None
        return {"content": content, "quote": quote}
