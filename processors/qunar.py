# coding=utf-8
import copy
from hashlib import md5
import gevent
import re
import pymongo
from mongo import get_mongodb, get_mysql_db
from gevent.queue import Queue

__author__ = 'zephyre'


class QunarPoiProcessor(object):
    name = 'qunar-poi'

    def __init__(self):
        super(QunarPoiProcessor, self).__init__()
        self.args = self.args_builder()
        self.total = None
        self.conn = None
        self.progress = 0
        self.prog_total = 0

    @staticmethod
    def args_builder():
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument('cmd')
        parser.add_argument('--limit', default=None, type=int)
        parser.add_argument('--skip', default=0, type=int)
        parser.add_argument('--cat', required=True, choices=['dining', 'shopping'], type=str)
        parser.add_argument('--query', type=str)
        parser.add_argument('--order', type=str)
        parser.add_argument('--concur', default=10, type=int)
        args, leftovers = parser.parse_known_args()
        return args

    def build_poi(self, entry, poi_type):
        poi_id = int(entry['id'])
        data = {'zhName': entry['name'], 'source': {'qunar': {'id': poi_id}},
                'location': {'type': 'Point', 'coordinates': [float(entry[key]) for key in ['lng', 'lat']]}}

        if entry['priceDesc']:
            try:
                price = int(entry['priceDesc'])
                data['price'] = price
            except ValueError:
                data['priceDesc'] = entry['priceDesc']

        for k1, k2 in [['addr', 'address'], ['openTime'] * 2, ['tel'] * 2, ['style'] * 2]:
            if entry[k1]:
                data[k2] = entry[k1]

        data['alias'] = [data['zhName'].lower()]

        col_country = get_mongodb('geo', 'Country', profile='mongo')
        ret = col_country.find_one({'alias': entry['countryName'].lower().strip()}, {'zhName': 1, 'enName': 1})
        assert ret is not None, 'Cannot find country: %s' % entry['countryName']
        data['country'] = ret

        col_loc = get_mongodb('geo', 'Locality', profile='mongo')
        ret = col_loc.find_one({'alias': re.compile(ur'^%s' % entry['distName'].lower().strip())},
                               {'zhName': 1, 'enName': 1})
        assert ret is not None, 'Cannot find city: %s' % entry['distName']
        data['locality'] = ret

        data['targets'] = [data['country']['_id'], data['locality']['_id']]

        if entry['tag']:
            data['tags'] = filter(lambda val: val, re.split(r'\s+', entry['tag']))

        if entry['special']:
            data['specials'] = filter(lambda val: val, re.split(r'\s+', entry['special']))

        cursor = self.conn.cursor()
        cursor.execute('SELECT COUNT(*) AS cnt FROM qunar_%s WHERE hotScore<%d' % (
            'meishi' if poi_type == 'dining' else 'gouwu', entry['hotScore']))
        data['hotness'] = float(cursor.fetchone()['cnt']) / self.total
        data['rating'] = data['hotness']

        col_im = get_mongodb('raw_qunar', 'Image', profile='mongo-raw')
        images = []
        for img in col_im.find({'poi_id': poi_id}).sort('ord', pymongo.ASCENDING).limit(10):
            images.append({'key': md5(img['url']).hexdigest()})
        if images:
            data['images'] = images

        return data

    def run(self):
        from datetime import datetime

        t1 = datetime.now()
        print t1
        self.conn = get_mysql_db('restore_poi', profile='mysql')

        args = self.args_builder()
        if args.cat == 'dining':
            table = 'qunar_meishi'
        elif args.cat == 'shopping':
            table = 'qunar_gouwu'
        else:
            assert False, 'Invalid table type: %s' % args.cat

        stmt = 'SELECT * FROM %s' % table

        tail = ''
        if args.limit or args.skip:
            import sys

            limit = args.limit if args.limit else sys.maxint
            offset = args.skip
            tail = ' LIMIT %d OFFSET %d' % (limit, offset)

        order = 'ORDER BY %s' % args.order if args.order else ''

        query = 'WHERE %s' % args.query if args.query else ''

        stmt = '%s %s %s %s' % (stmt, query, order, tail)

        cur = self.conn.cursor()

        cur.execute('SELECT COUNT(*) AS cnt FROM %s' % table)
        self.total = cur.fetchone()['cnt']

        cur.execute(stmt)
        self.prog_total = cur.rowcount

        tasks = Queue()
        for entry in cur:
            def func(val=entry):
                print 'Upserting %s' % val['name']
                data = self.build_poi(val, args.cat)
                col_name = 'Restaurant' if args.cat == 'dining' else 'Shopping'
                col = get_mongodb('poi', col_name, profile='mongo')
                col.update({'source.qunar.id': data['source']['qunar']['id']}, {'$set': data}, upsert=True)
                self.progress += 1

            tasks.put_nowait(func)

        def worker():
            while not tasks.empty():
                task = tasks.get()
                task()
                gevent.sleep(0)

        def timer():
            while not tasks.empty():
                print 'Progress: %d / %d' % (self.progress, self.prog_total)
                gevent.sleep(10)

        jobs = [gevent.spawn(timer)]
        for i in xrange(self.args.concur):
            jobs.append(gevent.spawn(worker))
        gevent.joinall(jobs)

        t2 = datetime.now()
        print t2
        print t2 - t1