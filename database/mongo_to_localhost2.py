# -*- coding: utf-8 -*-

import pymongo
import time
stroge_config = {
    "host": "172.22.67.35",
    "port": 20000
}

local_config = {
    "host": "127.0.0.1",
    "port": 27017
}

collect_list = list()


def collection_deal():
    client = pymongo.MongoClient(**stroge_config)
    db = client["spider_data"]
    collect_list = db.collection_names()
    client.close()
    return collect_list


def search_from_target(coll):
        client = pymongo.MongoClient(**stroge_config)
        db = client["spider_data"]
        collection = db[coll]
        result = collection.find()
        client.close()
        return result


def insert_to_local(coll, result):
    client = pymongo.MongoClient(**local_config)
    db = client["spider_data"]
    collection = db[coll]
    collection.insert_many(result)
    client.close()


def run():
    collect_list = collection_deal()
    # i = 1
    # for coll in collect_list:
    coll = "CREDITCARDACT_FINASSIST"
    result = search_from_target(coll)
    re_list = list()
    for re in result:
        re_list.append(re)

    insert_to_local(coll, re_list)
    print("done{}".format(i))
    i += 1


if __name__ == '__main__':
    client = pymongo.MongoClient(**stroge_config)
    db = client["spider_data"]
    collection = db["LIANJIA_FINASSIST"]
    # result = collection.find({"$and": [
    #                 {"ENTITY_CODE_": "LJHOUSE"},{"d": {"$exists": False}}]})
    try:
        count = collection.aggregate([{'$group': {'_id': '$PROVINCE_NAME_', 'count': {'$sum': 1}}}])
    except pymongo.errors.ServerSelectionTimeoutError:
        time.sleep(5)
        count = collection.aggregate([{'$group': {'_id': '$PROVINCE_NAME_', 'count': {'$sum': 1}}}])
    print(count)
    # count = collection.aggregate([{'$group': {'_id': '$PROVINCE_NAME_', 'count': {'$sum': 1}}}])
    # print(count)
    # run()
