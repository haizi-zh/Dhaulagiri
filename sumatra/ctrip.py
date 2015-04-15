# coding=utf-8
import re
from lxml import etree
from utils import build_href

__author__ = 'zephyre'


def parse_qa_list(body):
    """
    解析问答的列表：http://you.ctrip.com/asks/p1
    """
    refer_url = 'http://you.ctrip.com'
    root = etree.fromstring(body, parser=etree.HTMLParser())
    for href in root.xpath('//ul[@class="asklist"]/li[@data-href]/@data-href'):
        assert re.match(r'^/asks/[^/]+/\d+.html$', href) is not None
        url = build_href(refer_url, href)
        yield {'method': 'GET', 'url': url}


def parse_qa_pages(body):
    """
    解析问答列表的翻页：http://you.ctrip.com/asks/p1
    """
    root = etree.fromstring(body, parser=etree.HTMLParser())
    page_nums = set([])
    for href in root.xpath('//div[@class="pager_con cf"]/div[@class="pager_v1"]/a[@href]/@href'):
        m = re.search(r'/asks/p(\d+)$', href)
        if m:
            page_nums.add(int(m.group(1)))

    if not page_nums:
        return

    max_page = sorted(list(page_nums))[-1]
    for page in xrange(1, max_page + 1):
        url = 'http://you.ctrip.com/asks/p%d' % page
        yield {'method': 'GET', 'url': url}