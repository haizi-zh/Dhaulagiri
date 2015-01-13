# coding=utf-8
from lxml.html import HtmlElement

__author__ = 'zephyre'


def parse_etree(node, rules):
    """
    对HTML节点进行处理
    :param node: HTML节点。可以是HtmlElement，也可以是字符串
    :param rules: 处理HTML节点的流水线函数。返回(flag, new_node)。
                其中，如果flag为True，则立即返回new_node，不再进行后续的流水线动作。
    :return:
    """
    import lxml.html.soupparser as soupparser

    if isinstance(node, HtmlElement):
        dom = node
    else:
        dom = soupparser.fromstring(node)

    def func(node):
        # 去掉可能的最外层html节点
        if node.tag == 'html':
            return func(node[0]) if len(node) == 1 else None

        if node.text:
            node.text = node.text.strip()
        if node.tail:
            node.tail = node.tail.strip()

        for r in rules:
            should_return, new_node = r(node)
            if should_return:
                return new_node
            else:
                node = new_node

        for child in node:
            proc_child = func(child)
            if proc_child is None:
                child.getparent().remove(child)
            elif child != proc_child:
                child.getparent().replace(child, proc_child)

        return node

    return func(dom)