# -*- coding: utf-8 -*-
from random import random, randint

import pymongo

stroge_config = {
    "host": "storage01",
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


def find_one_and_update(coll):
    """
    find all data from MongoDB
    :param coll: collection like db[collection]
    :return: pymongo cursor object
    """
    client = pymongo.MongoClient(**stroge_config)
    db = client["spider_data"]
    collection = db[coll]
    while True:
        try:
            result = collection.find_one_and_update(filter={"d": {"$exists": True}}, update={"$unset": {"d": 0}})
            print(result)
        except:
            client.close()
            break
        else:
            if not result:
                break


def insert_to_local(coll, result):
    client = pymongo.MongoClient(**local_config)
    db = client["spider_data"]
    collection = db[coll]
    collection.insert_many(result)
    client.close()


def main():
    count = 0
    find_client = pymongo.MongoClient(**stroge_config)
    find_db = find_client["spider_data"]
    find_collection = find_db["WEIBOBASICINFO"]

    insert_client = pymongo.MongoClient(**local_config)
    insert_db = insert_client["spider_data"]
    insert_collection = insert_db["WEIBOBASICINFO"]

    # data = find_collection.find_one({"ENTITY_CODE_": "CIBORGANIZE"})
    data = find_collection.find_one()
    data_id = data["_id"]
    insert_collection.insert_one(data)
    count += 1
    while True:
        data_sec = find_collection.find_one({"$and": [{"_id": {"$gt": data_id}}]})
        if data_sec:
            data_id = data_sec["_id"]
            insert_collection.insert_one(data_sec)
            count += 1
            print(count)
        else:
            print("done", count)
            break


def run():
    # collect_list = collection_deal()
    # i = 1
    # for coll in collect_list:
    coll = "JRCP_XYK"
    # coll = "JRCP_JJ"
    result = find_one_and_update(coll)

    # re_list = list()
    # for re in result:
        # re.pop("_id")
        # del re["_id"]
        # re_list.append(re)
    # for m in range(101):
    #     feak_dict = dict()
    #     feak_dict["ENTITY_CODE_"] = "86JCW"
    #     for i in range(10):
    #         j = randint(1, 1000)
    #         k = randint(1, 1000)
    #         feak_dict[str(j)] = str(k)
    #         feak_dict["TITLE_"] = str(j)
    #     print(feak_dict)
    #     re_list.append(feak_dict)
    # insert_to_local(coll, re_list)
    # print("done{}".format(i))
    # i += 1


def aggre():
    client = pymongo.MongoClient(**stroge_config)
    db = client["spider_data"]
    # collection = db["mapbar_shanghai"]
    collection = db["WD_TY"]
    # result = collection.aggregate([{"$match": {"BTYPE_": "交通设施"}}, {"$project": {"_id": 0, "TYPE_": 1}}, {"$group": {"_id": "$TYPE_"}}])
    result = collection.aggregate([{"$project": {"_id": 0, "SOURCE_TYPE_NAME_": 1}}, {"$group": {"_id": "$SOURCE_TYPE_NAME_"}}])
    while True:
        try:
            data = next(result)
            print(data)
        except StopIteration:
            break
        # if data["_id"][-2:] == "道路":
        # if "道路" in data["_id"]:
        #     print(data)


if __name__ == '__main__':
    # aggre()
    run()
