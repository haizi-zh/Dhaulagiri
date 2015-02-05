# coding=utf-8

import pymongo
from bson.objectid import ObjectId
from processors import BaseProcessor
from utils.database import get_mongodb

class Coordings(BaseProcessor):

    name = 'coordings'

    def __init__(self, *args, **kwargs):
        BaseProcessor.__init__(self, *args, **kwargs)

    def populate_tasks(self):
        print 'hello world'
        col_poi = get_mongodb('poi', "ViewSpot", 'mongo-raw')
        query = {'taoziEna': True}
        cursor = list(col_poi.find(query, {"location": 1}).sort('hotness', pymongo.DESCENDING).limit(10))

        coordstr = ''
        for idx, val in enumerate(cursor):
            latlng = val['location']['coordinates']
            coordstr += latlng[1] + ',' + latlng[0]

        print coordstr
