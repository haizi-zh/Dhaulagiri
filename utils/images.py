import hashlib
import re

__author__ = 'zephyre'


class MfwImageExtractor(object):
    """
    Extract image urls from Mafengwo records
    """
    def __init__(self):
        def helper(image_id, src):
            key = hashlib.md5(src).hexdigest()

            return {'id': image_id, 'metadata': {}, 'src': src, 'key': key, 'url_hash': key}

        def f1(src):
            pattern = r'([^\./]+)\.\w+\.[\w\d]+\.(jpeg|bmp|png)$'
            match = re.search(pattern, src)
            if not match:
                return None
            c = match.group(1)
            ext = match.group(2)
            src = re.sub(pattern, '%s.%s' % (c, ext), src)
            return helper(c, src)

        self.extractor = [f1]

    def retrieve_image(self, src):
        for func in self.extractor:
            ret = func(src)
            if ret:
                return ret


class BaiduImageExtractor(object):
    """
    Extract image urls from Baidu records
    """
    def __init__(self):
        def helper(image_id, src):
            key = hashlib.md5(src).hexdigest()

            return {'id': image_id, 'metadata': {}, 'src': src, 'key': key, 'url_hash': key}

        def f1(src):
            match = re.search(r'hiphotos\.baidu\.com/lvpics/(pic|abpic)/item/([0-9a-f]{40})\.jpg', src)
            if not match:
                return None
            c = match.group(2)
            src = 'http://hiphotos.baidu.com/lvpics/pic/item/%s.jpg' % c
            return helper(c, src)

        def f2(src):
            match = re.search(r'himg\.bdimg\.com/sys/portrait/item/(\w+)\.jpg', src)
            if not match:
                return None
            return helper(match.group(1), src)

        def f3(src):
            match = re.search(r'hiphotos\.baidu\.com/lvpics/.+sign=[0-9a-f]+/([0-9a-f]{40})\.jpg', src)
            if not match:
                return None
            c = match.group(1)
            src = 'http://hiphotos.baidu.com/lvpics/pic/item/%s.jpg' % c
            return helper(c, src)

        self.extractor = [f1, f2, f3]

    def retrieve_image(self, src):
        for func in self.extractor:
            ret = func(src)
            if ret:
                return ret