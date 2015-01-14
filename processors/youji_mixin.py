# coding=utf-8
import urlparse
from utils.database import get_mongodb
from utils.images import BaiduImageExtractor, MfwImageExtractor


class TravelNoteMixin(object):
    def __init__(self):
        pass

    def image_proc(self, ret):
        # 检查是否已经存在于数据库中
        data = {}
        col_im = get_mongodb('imagestore', 'Images', 'mongo')
        col_cand = get_mongodb('imagestore', 'ImageCandidates', 'mongo')
        img = col_im.find_one({'url_hash': ret['url_hash']}, {'_id': 1})
        if not img:
            img = col_cand.find_one({'url_hash': ret['url_hash']}, {'_id': 1})
        if not img:
            # 添加到待抓取列表中
            data = {'key': ret['key'], 'url': ret['src'], 'url_hash': ret['url_hash']}
        return data

    def br_proc(self, sub_node):  # 删除br标签
        n_parent = sub_node.getparent()
        for j in range(0, len(n_parent)):
            if n_parent[j].tag == 'br':
                del n_parent[j]
                break

    def attr_clear(self, sub_node):  # 处理标签的所有属性
        sub_node.attrib.clear()

    def a_proc(self, sub_node, netloc):  # 处理a标签
        if sub_node.attrib.keys():
            for attr in sub_node.attrib.keys():
                if attr == 'href':
                    url = sub_node.attrib[attr]
                    ret = urlparse.urlparse(url)
                    if ret.netloc == '' or ret.netloc == netloc:
                        sub_node.attrib.pop(attr)
                    else:
                        continue
                elif attr != 'target':
                    sub_node.attrib.pop(attr)

    def span_proc(self, sub_node):
        """
        处理span标签
        :param sub_node:
        """
        if sub_node.attrib.keys():
            for attr in sub_node.attrib.keys():
                if attr == 'class':
                    if sub_node.attrib[attr] == 'des-l':  # 照片描述
                        sub_node.attrib[attr] = 'notes-photo-desc'
                    elif sub_node.attrib[attr] == 'scene-r':  # 拍摄地描述
                        sub_node.attrib[attr] = 'notes-photo-loc'
                    else:
                        sub_node.attrib.pop(attr)
                else:
                    sub_node.attrib.pop(attr)


class BaiduDomTreeProc(TravelNoteMixin, BaiduImageExtractor):
    """
    百度游记标签处理
    """

    def __init__(self):
        TravelNoteMixin.__init__(self)
        BaiduImageExtractor.__init__(self)

    def img_proc(self, sub_node):  # 处理img标签
        ret = None
        if sub_node.attrib.keys():
            for attr in sub_node.attrib.keys():
                if attr == 'src':
                    # log.msg('wait', level=log.INFO)
                    ret = self.retrieve_image(sub_node.attrib['src'])
                    if not ret:
                        sub_node.attrib.pop(attr)
                        continue
                    else:
                        sub_node.attrib['photo-id'] = ret['key']
                        sub_node.attrib.pop('src')

                elif attr == 'class':
                    if sub_node.attrib['class'] == 'notes-photo-img':
                        sub_node.attrib['class'] = 'notes-photo'
                    else:
                        sub_node.attrib.pop(attr)
                else:
                    sub_node.attrib.pop(attr)
        return ret

    # 遍历树,去除内部跳转链接,抽取图片url
    def walk_tree(self, root, ret_list):
        if not len(root):
            return None
        for node in root.iter():
            if node.tag == 'a':
                self.a_proc(node, 'lvyou.baidu.com')
            elif node.tag == 'img':
                ret_tmp = self.img_proc(node)
                if ret_tmp:
                    ret_list.append(ret_tmp)
            elif node.tag == 'span':
                self.span_proc(node)
            elif node.tag == 'p' or node.tag == 'div':
                self.attr_clear(node)
            elif node.tag == 'br':
                self.br_proc(node)

        return {'root': root, 'ret_list': ret_list}


class MfwDomTreeProc(TravelNoteMixin, MfwImageExtractor):
    def __init__(self):
        TravelNoteMixin.__init__(self)
        MfwImageExtractor.__init__(self)

    def img_proc(self, sub_node):  # 处理img标签
        ret = None
        if sub_node.attrib.keys():
            for attr in sub_node.attrib.keys():
                if attr == 'src':
                    # log.msg('wait', level=log.INFO)
                    ret = self.retrieve_image(sub_node.attrib['src'])
                    if not ret:
                        sub_node.attrib.pop(attr)
                        continue
                    else:
                        sub_node.attrib['photo-id'] = ret['key']
                        sub_node.attrib.pop('src')

                elif attr == 'class':
                    if sub_node.attrib['class'] == 'notes-photo-img':
                        sub_node.attrib['class'] = 'notes-photo'
                    else:
                        sub_node.attrib.pop(attr)
                else:
                    sub_node.attrib.pop(attr)
        return ret

    # 遍历树,去除内部跳转链接,抽取图片url
    def walk_tree(self, root, ret_list):
        if not len(root):
            return None
        for node in root.iter():
            if node.tag == 'a':
                self.a_proc(node, 'www.mafengwo.com')
            elif node.tag == 'img':
                ret_tmp = self.img_proc(node)
                if ret_tmp:
                    ret_list.append(ret_tmp)
            elif node.tag == 'span':
                self.span_proc(node)
            elif node.tag == 'p' or node.tag == 'div':
                self.attr_clear(node)
            elif node.tag == 'br':
                self.br_proc(node)
            else:
                self.attr_clear(node)

        return {'root': root, 'ret_list': ret_list}

