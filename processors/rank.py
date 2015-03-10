# coding=utf-8

import pymongo
from bson.objectid import ObjectId
from processors import BaseProcessor
from utils.database import get_mongodb


class PoiRank(BaseProcessor):
    name = "poi-rank"

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)

    def __id_generator(self, collections_array):
        db_conns = []
        for coon_name in collections_array:
            db_conns.append(get_mongodb('poi', coon_name, 'mongo'))
        for conn in db_conns:
            for sid in conn.distinct('locality._id'):
                self.logger.info(sid)
                yield {'conn': conn, 'id': sid}

    def populate_tasks(self):
        conn_names = ['Shopping', 'Restaurant', 'Hotel']

        for info in self.__id_generator(conn_names):
            db_conn = info['conn']
            sid = info['id']
            query = {'locality._id': ObjectId(sid)}
            cursor = list(db_conn.find(query, {"_id": 1}).sort('hotness', pymongo.DESCENDING))
            for idx, val in enumerate(cursor):
                def func(entry=val, flag=idx):
                    self.logger.info('do -- %s' % entry['_id'])
                    db_conn.update({"_id": entry['_id']}, {'$set': {'rank': flag + 1}})
                self.add_task(func)

        print '\n'