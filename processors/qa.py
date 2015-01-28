# coding=utf-8
from lxml import etree
import re
import time
import pysolr

from scrapy import Selector

from processors import BaseProcessor

from utils.database import get_mongodb, get_solr


__author__ = 'lxf'


class CtripProc(BaseProcessor):
    name = 'ctrip_proc'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)
        self.args = self.args_builder()

    def args_builder(self):
        parser = self.arg_parser
        parser.add_argument('--limit', default=0, type=int)
        parser.add_argument('--skip', default=0, type=int)
        parser.add_argument('--query', type=str)
        return parser.parse_args()

    # 处理回答
    def proc_answer(self, item):
        data = {}
        a_id = item['a_id'] if 'a_id' in item else None  # 问题的id
        q_id = item['q_id'] if 'q_id' in item else None  # 问题的id
        qId = 'ctrip_q_%s' % q_id  # 标明源
        data['source'] = 'ctip_a_%s' % a_id
        data['qId'] = qId
        essence = item['rec'] if 'rec' in item else False
        data['essence'] = essence
        # 处理body内容
        q_body = item['body'] if 'body' in item else None
        if not q_body:
            return None
        else:
            sel = Selector(text=q_body)  # 从创建选择器实例
            # 用户名
            authorName = sel.xpath('//p[@class="answer_user"]/a/text()').extract()
            data['authorName'] = authorName[0] if authorName else None
            # 用户头像
            authorAvatar = sel.xpath('//a[@class="answer_img"]/img/@src').extract()
            data['authorAvatar'] = authorAvatar[0] if authorAvatar else None
            # 发表时间
            publishTime = sel.xpath('//span[@class="answer_time"]/text()').extract()
            if publishTime:
                tmp_match = re.search(u'前', publishTime[0])  # 判断是标准时间
                if not tmp_match:
                    # 正则匹配
                    match = re.search(r'(\d+)-(\d+)-(\d+)\s(\d+):(\d+):(\d+)', publishTime[0])
                    if match:
                        tmp_time = match.group()
                        publishTime = long(time.mktime(time.strptime(tmp_time, '%Y-%m-%d %H:%M:%S')) * 1000)
                        data['publishTime'] = publishTime
                    else:
                        data['publishTime'] = None
                else:
                    tmp_time = '2015-01-26 10:20:34'  # 模糊时间
                    publishTime = long(time.mktime(time.strptime(tmp_time, '%Y-%m-%d %H:%M:%S')) * 1000)
                    data['publishTime'] = publishTime
            else:
                data['publishTime'] = None

            # 回答内容
            answer_tmp = sel.xpath('//p[@class="answer_text"]/text()').extract()
            if len(answer_tmp) > 1:
                data['contents'] = reduce(lambda x, y: '%s%s' % (x, y), answer_tmp)
            else:
                data['contents'] = answer_tmp[0] if answer_tmp else None
            # voteCnt
            voteCnt = sel.xpath('//div[@class="answer_comment cf"]//a//span/text()').extract()
            voteCnt = re.search(r'\d+', voteCnt[0]).group() if voteCnt else 0
            data['voteCnt'] = int(voteCnt)

        return data

    # 处理问题
    def proc_question(self, item):
        data = {}
        q_id = item['q_id'] if 'q_id' in item else None  # 问题的id
        source = 'ctrip_q_%s' % q_id  # 标明源
        data['source'] = source
        # 处理body内容
        q_body = item['body'] if 'body' in item else None
        if not q_body:
            return None
        else:
            prefix = '//div[@class="detailmain_top"]'
            sel = Selector(text=q_body)  # 从创建选择器实例
            # 用户名
            authorName = sel.xpath('%s//a[@class="ask_username"]/text()' % prefix).extract()
            data['authorName'] = authorName[0] if authorName else None
            # 发表时间
            publishTime = sel.xpath('%s//span[@class="ask_time"]/text()' % prefix).extract()
            if publishTime:
                tmp_match = re.search(u'前', publishTime[0])  # 判断是标准时间
                if not tmp_match:
                    # 正则匹配
                    match = re.search(r'(\d+)-(\d+)-(\d+)\s(\d+):(\d+):(\d+)', publishTime[0])
                    if match:
                        tmp_time = match.group()
                        publishTime = long(time.mktime(time.strptime(tmp_time, '%Y-%m-%d %H:%M:%S')) * 1000)
                        data['publishTime'] = publishTime
                    else:
                        data['publishTime'] = None
                else:
                    tmp_time = '2015-01-26 10:20:34'  # 模糊时间
                    publishTime = long(time.mktime(time.strptime(tmp_time, '%Y-%m-%d %H:%M:%S')) * 1000)
                    data['publishTime'] = publishTime
            else:
                data['publishTime'] = None

            # 提问标题
            tmp_title = sel.xpath('%s//h1[@class="ask_title"]/text()[2]' % prefix).extract()
            data['title'] = tmp_title[0] if tmp_title else None
            # 提问内容
            ask_tmp = sel.xpath('%s/p[@class="ask_text"]/text()' % prefix).extract()
            data['contents'] = ''.join(ask_tmp[0].split()) if ask_tmp else None
            # 提取标签
            tags = sel.xpath('%s//div[@class="asktag_oneline cf"]/a/@title' % prefix).extract()
            data['tags'] = tags if tags else None

        return data

    def populate_tasks(self):
        # 链接mongo
        query = self.args.query  # answer   question
        if query == 'answer':
            arg = 'Answer'
        elif query == 'question':
            arg = 'Question'
        else:
            raise AttributeError
        raw_ans_col = get_mongodb('raw_faq', arg, 'mongo-raw')
        col = get_mongodb('misc', arg, 'mongo')
        for item in raw_ans_col.find():
            def func(entry=item):
                # 处理item
                if query == 'answer':
                    data = self.proc_answer(entry)
                if query == 'question':
                    data = self.proc_question(entry)
                if data:
                    col.update({'source': data['source']}, {'$set': data}, upsert=True)

            self.add_task(func)


class QuestionSolr(BaseProcessor):
    name = 'question_solr'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)
        self.args = self.args_builder()

    def args_builder(self):
        parser = self.arg_parser
        parser.add_argument('--limit', default=0, type=int)
        parser.add_argument('--skip', default=0, type=int)
        parser.add_argument('--query', type=str)
        return parser.parse_args()

    def proc_item(self, item):
        data = {'id': str(item['_id']), 'title': item['title'],
                'title': item['title'] if 'title' in item else None,
                'publishTime': item['publishTime'] if 'publisTime' in item else None,
                'authorName': item['authorName'] if 'authorName' in item else None,
                'contents': item['contents'] if 'contents' in item else None,
                'tags': item['tags'] if 'tags' in item else None,
                'authorAvatar': item['authorAvatar'] if 'authorAvatar' in item else None,
                'source': item['source'] if 'source' in item else None
        }
        return data

    def populate_tasks(self):
        col = get_mongodb('misc', 'Question', 'mongo')
        solr_s = get_solr('qa')
        for entry in col.find():
            def func(item=entry):
                data = self.proc_item(item)
                if data:
                    doc = [data]
                    try:
                        solr_s.add(doc)
                    except pysolr.SolrError, e:
                        self.log('error:%s,id:%s' % (e.message, data['id']))

            self.add_task(func)


class QunarProc(BaseProcessor):
    """
    去哪网的数据处理
    """
    name = 'qunar_proc'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)
        self.args = self.args_builder()

    def args_builder(self):
        parser = self.arg_parser
        parser.add_argument('--limit', default=0, type=int)
        parser.add_argument('--skip', default=0, type=int)
        parser.add_argument('--query', type=str)
        return parser.parse_args()

    def proc_data(self, item):  # 更改处理方式，同步处理问题与答案
        answer_list = []
        question_data = {}
        q_id = item['q_id'] if 'q_id' in item else None
        if not q_id:
            return None
        body = item['body'] if 'body' in item else None
        if not body:
            return None

        # 问题处理
        question_data['source'] = 'qunar_%s' % q_id
        question_data['title'] = item['title'] if 'title' in item else None
        root = etree.HTML(body)
        # 用户名
        authname_root = root.xpath('//div[contains(@id,"post")]//td[@class="pls"]//div[@class="pi"]//a')
        authorName = authname_root[0].text
        # 用户头像
        authavatar_root = root.xpath(
            '//div[contains(@id,"post")]//td[@class="pls"]//div[@class="avatar"]//img')
        authorAvatar = authavatar_root[0].attrib['src']
        # 获取内容
        content_root = root.xpath(
            '//div[contains(@id,"post")]//td[@class="plc"]//div[@class="pct"]//td[@class="t_f"]')
        text = []
        for tmp in content_root[0].itertext():
            if tmp.strip():
                text.append(tmp.strip())
        contents = ''.join(text)
        # 发表时间
        publish_time_root = root.xpath(
            '//div[contains(@id,"post")]//td[@class="plc"]//div[@class="pi"]//div[@class="authi"]/em/span')
        publish_time = publish_time_root[0].attrib['title']
        publishTime = long(
            time.mktime(time.strptime(publish_time, '%Y-%m-%d %H:%M:%S')) * 1000) if publish_time else None
        # merge
        question_data['authorName'] = authorName
        question_data['authorAvatar'] = authorAvatar
        question_data['contents'] = contents
        question_data['publishTime'] = publishTime

        # 获取答案
        answer_col = get_mongodb('raw_faq', 'QunarAnswer', 'mongo-raw')
        for entry in answer_col.find({'q_id': q_id}):
            data = {}
            tmp_body = entry['body'] if 'body' in entry else None
            if tmp_body:
                # 判断是否进行交互，存在交互不进行处理
                root = etree.HTML(body)
                quote_root = root.xpath(
                    '//div[contains(@id,"post")]//td[@class="plc"]//div[@class="pct"]//td[@class="t_f"]/div[@class="quote"]')
                if quote_root:
                    continue
                else:
                    # 帖子的id
                    post_id = entry['post_id']
                    data['source'] = 'qunar_%s' % post_id
                    # 提问的id
                    q_id = entry['q_id']
                    data['qId'] = q_id
                    # 用户名
                    authname_root = root.xpath('//div[contains(@id,"post")]//td[@class="pls"]//div[@class="pi"]//a')
                    authorName = authname_root[0].text
                    # 用户头像
                    authavatar_root = root.xpath(
                        '//div[contains(@id,"post")]//td[@class="pls"]//div[@class="avatar"]//img')
                    authorAvatar = authavatar_root[0].attrib['src']
                    # 获取问题内容
                    content_root = root.xpath(
                        '//div[contains(@id,"post")]//td[@class="plc"]//div[@class="pct"]//td[@class="t_f"]')
                    text = []
                    for tmp in content_root[0].itertext():
                        if tmp.strip():
                            text.append(tmp.strip())
                    contents = ''.join(text)
                    # 发表时间
                    publish_time_root = root.xpath(
                        '//div[contains(@id,"post")]//td[@class="plc"]//div[@class="pi"]//div[@class="authi"]/em/span')
                    publish_time = publish_time_root[0].attrib['title']
                    publishTime = long(
                        time.mktime(time.strptime(publish_time, '%Y-%m-%d %H:%M:%S')) * 1000) if publish_time else None
                    data['authorName'] = authorName
                    data['authorAvatar'] = authorAvatar
                    data['contents'] = contents
                    data['publishTime'] = publishTime
                    # 填加回答
                    answer_list.append(data)

        return {'question': question_data, 'answer': answer_list}

    def populate_tasks(self):
        raw_ques_col = get_mongodb('raw_faq', 'QunarQuestion', 'mongo-raw')
        ques_col = get_mongodb('misc', 'QunarQuestion', 'mongo')
        answer_col = get_mongodb('misc', 'QunarAnswer', 'mongo')
        for item in raw_ques_col.find():
            def func(entry=item):
                result = self.proc_data(entry)
                question_data = result['question_data']
                answer_list = result['answer_list']
                if question_data:
                    ques_col.update({'source': question_data['source']}, {'$set': question_data}, upsert=True)
                if answer_list:
                    for node in answer_list:
                        answer_col.update({'source': node['source']}, {'$set': node}, upsert=True)

            self.add_task(func)








