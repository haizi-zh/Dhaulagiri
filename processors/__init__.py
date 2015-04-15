# coding=utf-8
__author__ = 'zephyre'


class BaseProcessor(object):
    def run(self, **kwargs):
        """
        生成任务
        """
        raise NotImplementedError


from celery import task
from billiard import current_process

@task
def get_worker_name():
    p = current_process()
    return p.initargs[1].split('@')[1]
