import pymongo
import time
from tools.read_excel import to_csv


stroge_config = {
    "host": "172.22.69.35",
    "port": 20000
}


def get_mongo_collection():
    collect_list = list()
    client = pymongo.MongoClient(**stroge_config)
    try:
        db = client["spider_data_old"]
        collect_list = db.collection_names()
    except pymongo.errors.ServerSelectionTimeoutError:
        time.sleep(2)
        db = client["spider_data_old"]
        collect_list = db.collection_names()
    client.close()
    return collect_list


def get_mongo_data(col):
    client = pymongo.MongoClient(**stroge_config)
    try:
        db = client["spider_data_old"]
        collection = db[col]
        result = collection.find({"$and": [{"PRICE_": {"$lte": "18500"}}, {"AREA_NAME_": "高新"}]})

        return result
    except pymongo.errors.ServerSelectionTimeoutError:
        time.sleep(2)
        db = client["spider_data_old"]
        collection = db[col]
        result = collection.find({"$and": [{"PRICE_": {"$lte": "18500"}},{"AREA_NAME_": "高新"}]})
    client.close()
    return result


if __name__ == '__main__':
    cdlianjia = get_mongo_data('WD_JZ_FJ_CD')
    while True:
        try:
            data = cdlianjia.__next__()
            print(data)
            if "YEAR_" in data:
                data["YEAR_"] = data["YEAR_"].replace("年建成", "")
            if "PRICE_" in data:
                data["PRICE_"] = data["PRICE_"].replace("元/平米", "")
            to_csv(data)
        except StopIteration:
            break