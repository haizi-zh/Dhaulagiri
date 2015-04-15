# coding=utf-8
import re

from lxml import etree

from utils import build_href


__author__ = 'zephyre'


def parse_scene_albums(body):
    """
    解析景点的相册：http://lvyou.baidu.com/xian/fengjing
    """
    root = etree.fromstring(body, parser=etree.HTMLParser())
    for href in root.xpath('//ul[@id="photo-list"]/li[@class="photo-item"]/a[@href]/@href'):
        m = re.search(r'.+/([0-9a-f]{24})', href)
        assert m is not None
        key = m.group(1)
        yield {'key': key, 'url': 'http://hiphotos.baidu.com/lvpics/pic/item/%s.jpg' % key}


def parse_scene_albums_pages(url, body):
    """
    解析景点相册的分页：http://lvyou.baidu.com/xian/fengjing
    """
    root = etree.fromstring(body, parser=etree.HTMLParser())
    page_nums = set([])
    href_template = None
    for href in root.xpath('//div[@class="pagelist-wrapper"]/span[@class="pagelist"]/a[@href]/@href'):
        m = re.search(r'.+/fengjing/\?pn=(\d+)', href)
        assert m is not None
        page_nums.add(int(m.group(1)))

        if not href_template:
            href_template = re.sub(r'\?pn=\d+', '?pn=%d', href)

    if not page_nums:
        return

    max_page = sorted(list(page_nums))[-1]
    for page in xrange(24, max_page + 1, 24):
        page_url = build_href(url, href_template % page)
        yield {'method': 'GET', 'url': page_url}