#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: winton 
@time: 2017-03-27 16:15 
"""
import pymongo
import codecs

import settings
from util import slaves


def action_to_mongo(filename, jobs=15):
    def _task(id, queue, context, start, end):
        with pymongo.MongoClient(settings.db_host) as client:
            db = client[settings.db_name]
            coll = db.action
            for line in context[start:end]:
                data = line.strip().split(',')
                # print(data)
                coll.insert({
                    'user_id': data[0],
                    'sku_id': int(data[1]),
                    'time': data[2],
                    'model_id': data[3],
                    'type': int(data[4]),
                    'cate': int(data[5]),
                    'brand': int(data[6])
                })
        print('{}: Success from {} to {}.'.format(id, start, end))

    with codecs.open(filename, 'r', 'gbk') as f:
        head = f.readline()
        print(filename)
        # print(head)
        # user_id,sku_id,time,model_id,type,cate,brand
        datas = f.readlines()
        count = len(datas)
        wg = slaves.Workgroup(jobs, _task)
        wg.work(datas, 0, count)
        wg.wait()


def comment_to_mongo():
    with pymongo.MongoClient(settings.db_host) as client:
        with codecs.open('./data/JData_Comment(修正版).csv', 'r', 'gbk') as f:
            db = client[settings.db_name]
            coll = db.comment
            coll.remove()
            head = f.readline()
            # print(head)
            # dt,sku_id,comment_num,has_bad_comment,bad_comment_rate
            num = 0
            for line in f.readlines():
                if num % 10000 == 0:
                    print(num)
                num += 1
                data = line.strip().split(',')
                # print(data)
                coll.insert({
                    'dt': data[0],
                    'sku_id': int(data[1]),
                    'comment_num': int(data[2]),
                    'has_bad_comment': int(data[3]),
                    'bad_comment_rate': float(data[4]),
                })


def product_to_mongo():
    with pymongo.MongoClient(settings.db_host) as client:
        with codecs.open('./data/JData_Product.csv', 'r', 'gbk') as f:
            db = client[settings.db_name]
            coll = db.product
            coll.remove()
            head = f.readline()
            # print(head)
            # sku_id, attr1, attr2, attr3, cate, brand
            num = 0
            for line in f.readlines():
                if num % 10000 == 0:
                    print(num)
                num += 1
                data = line.strip().split(',')
                # print(data)
                coll.insert({
                    'sku_id': int(data[0]),
                    'attr1': int(data[1]),
                    'attr2': int(data[2]),
                    'attr3': int(data[3]),
                    'cate': int(data[4]),
                    'brand': int(data[5])
                })


def user_to_mongo():
    with pymongo.MongoClient(settings.db_host) as client:
        with codecs.open('./data/JData_User.csv', 'r', 'gbk') as f:
            db = client[settings.db_name]
            coll = db.user
            coll.remove()
            head = f.readline()
            # print(head)
            # user_id, age, sex, user_lv_cd, user_reg_dt
            num = 0
            for line in f.readlines():
                if num % 10000 == 0:
                    print(num)
                num += 1
                data = line.strip().split(',')
                # print(data)
                coll.insert({
                    'user_id': int(data[0]),
                    'age': data[1],
                    'sex': int(data[2]),
                    'user_lv_cd': int(data[3]),
                    'user_reg_dt': data[4]
                })


if __name__ == '__main__':
    # user_to_mongo()
    # product_to_mongo()
    # comment_to_mongo()
    # action_to_mongo('./data/JData_Action_201602.csv')
    action_to_mongo('./data/JData_Action_201603.csv')
    action_to_mongo('./data/JData_Action_201603_extra.csv')
    action_to_mongo('./data/JData_Action_201604.csv')
