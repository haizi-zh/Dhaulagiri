# coding=utf-8

import pymongo
from bson.objectid import ObjectId
from processors_old import BaseProcessor
from utils.database import get_mongodb


class PoiRank(BaseProcessor):
    name = "poi-rank"

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)

    def populate_tasks(self):

        # extract ids
        fin = open('/root/Dhaulagiri/tempfile/online_city_raw.txt', 'r')
        fout = open('/root/Dhaulagiri/tempfile/online_city_final.txt','a')
        id_list = []
        id_kv_name = {}

        for line in fin.readlines():
            temp_id = line[20:44]
            id_list.append(temp_id)
            temp_name = line.split(' ')[6].replace('"', '')
            id_kv_name[temp_id] = temp_name
            fout.write(('%s %s\n') % (temp_id, temp_name))

        fin.close()
        fout.close()
        print id_list

        # Restaurant
        cols = ["Shopping", "Hotel"]
        for col in cols:
            print 'Begin %s' % col
            for loc_id in id_list:
                col_poi = get_mongodb('poi', col, 'mongo')
                query = {'taoziEna': True, 'locality._id': ObjectId(loc_id)}
                cursor = list(col_poi.find(query, {"_id": 1}).sort('hotness', pymongo.DESCENDING))

                if 0 == len(cursor):
                    print ('%s %s can\'t be find with "locality._id"') % (loc_id, id_kv_name[loc_id])
                    continue

                for idx, val in enumerate(cursor):
                    def func(entry=val, flag=idx):
                        col_poi.update({"_id": entry['_id']}, {'$set': {'rank': flag + 1}})

                    self.add_task(func)
            print '\n'