import requests

import baidu

import ctrip
import qunar


__author__ = 'zephyre'

from unittest import TestCase


# @unittest.skip('')
class TestCtrip(TestCase):
    def test_qa_list(self):
        response = requests.get('http://you.ctrip.com/asks/p1')

        import ctrip

        qa_list = list(ctrip.parse_qa_list(response.text))
        self.assertEqual(len(qa_list), 15)

    def test_qa_list_pages(self):
        response = requests.get('http://you.ctrip.com/asks/p1')

        from sumatra import ctrip

        qa_list = list(ctrip.parse_qa_pages(response.text))
        self.assertGreater(len(qa_list), 300)


# @unittest.skip('')
class TestBaidu(TestCase):
    def test_album_list(self):
        response = requests.get('http://lvyou.baidu.com/xian/fengjing?pn=0')

        import baidu

        album_list = list(baidu.parse_scene_albums(response.text))
        self.assertEqual(len(album_list), 24)

    def test_album_pages(self):
        url = 'http://lvyou.baidu.com/xian/fengjing?pn=0'
        response = requests.get(url)

        from sumatra import baidu

        page_list = list(baidu.parse_scene_albums_pages(url, response.text))
        self.assertGreaterEqual(len(page_list), 1)


class TestQunar(TestCase):
    def test_poi_comments(self):
        response = requests.get(
            'http://travel.qunar.com/place/api/html/comments/poi/722963?sortField=1&img=false&pageSize=10&page=1')

        import qunar

        comment_list = list(qunar.poi_comments_list(response.text))

        self.assertEqual(len(comment_list), 10)

    def test_poi_comments_paging(self):
        response = requests.get(
            'http://travel.qunar.com/place/api/html/comments/poi/722963?sortField=1&img=false&pageSize=10&page=1')

        from sumatra import qunar

        max_page = qunar.poi_comments_max_page(response.text)

        self.assertGreater(max_page, 0)
