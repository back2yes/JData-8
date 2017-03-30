#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: winton 
@time: 2017-03-29 10:37 
"""
import pymongo

import settings


def aggregate(table, name):
    result = table.aggregate([{'$group': {'_id': '$' + name, 'count': {'$sum': 1}}},
                              {'$sort': {'count': -1}}])
    print('### ' + name)
    for i in result:
        print('|{}|{}|'.format(i['_id'], i['count']))


if __name__ == '__main__':
    with pymongo.MongoClient(settings.db_host) as client:
        db = client[settings.db_name]
        # # user
        # coll_user = db.user
        # aggregate(coll_user, 'age')
        # aggregate(coll_user, 'sex')
        # aggregate(coll_user, 'user_lv_cd')
        # aggregate(coll_user, 'user_reg_dt')
        # # product
        # coll_product = db.product
        # aggregate(coll_product, 'attr1')
        # aggregate(coll_product, 'attr2')
        # aggregate(coll_product, 'attr3')
        # aggregate(coll_product, 'cate')
        # # comment
        # coll_comment = db.comment
        # aggregate(coll_comment, 'dt')
        # aggregate(coll_comment, 'comment_num')
        # aggregate(coll_comment, 'has_bad_comment')
        # action
        coll_action = db.action
        aggregate(coll_action, 'type')
        aggregate(coll_action, 'cate')
        aggregate(coll_action, 'model_id')
