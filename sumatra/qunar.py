# coding=utf-8
import json
import re
from datetime import datetime

from lxml import etree


__author__ = 'zephyre'


def poi_comments_list(body):
    """
    解析去哪儿POI的点评：http://travel.qunar.com/place/api/html/comments/poi/722963?sortField=1&img=false&pageSize=10&
    page=1
    """
    data = json.loads(body)['data']
    root = etree.fromstring(data, parser=etree.HTMLParser())
    for entry in root.xpath('//ul[@id="comment_box"]/li[@id]'):
        m = re.search(r'cmt_item_(\d+)', entry.xpath('./@id')[0])
        assert m is not None
        cmt_id = int(m.group(1))

        title = entry.xpath('.//a[@data-beacon="comment_title"]/text()')[0]

        tmp = entry.xpath('.//span[@class="total_star"]/span[contains(@class,"star")]/@class')[0]
        m = re.search(r'star_(\d)', tmp)
        assert m is not None
        rating = float(m.group(1)) / 5

        contents = ''.join(entry.xpath('.//div[@class="e_comment_content"]')[0].itertext())

        comment = {'id': cmt_id, 'title': title, 'rating': rating, 'contents': contents}

        tmp = entry.xpath('.//div[@class="e_comment_add_info"]/ul/li//a[@href and @data-beacon="comment_travelbook"]')
        if tmp:
            node = tmp[0]
            note_title = node.xpath('./text()')[0]
            m = re.search(r'/gonglve/(\d+)', node.xpath('./@href')[0])
            assert m is not None
            note_id = int(m.group(1))
            comment['note_title'] = note_title
            comment['note_id'] = note_id

        tmp = entry.xpath('.//div[@class="e_comment_add_info"]/ul/li/text()')
        if tmp:
            comment['time'] = datetime.strptime(tmp[0], '%Y-%m-%d')

        yield comment

    pass


def poi_comments_max_page(body):
    """
    解析去哪儿POI的点评的分页：http://travel.qunar.com/place/api/html/comments/poi/722963?sortField=1&img=false&
    pageSize=10&page=1
    """
    data = json.loads(body)['data']
    root = etree.fromstring(data, parser=etree.HTMLParser())
    page_list = set([])
    for val in root.xpath('//div[@class="b_paging"]/a[@class="page" and @data-url and @href]/text()'):
        m = re.match(r'\d+', val)
        if m:
            page_list.add(int(m.group()))

    return max(page_list)
