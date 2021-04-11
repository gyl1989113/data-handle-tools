import pymongo
from bson.objectid import ObjectId
from __config import *


class Mongoclient(object):
    def __init__(self, entity_code=None, mongo_collection=None):
        self.mongo_host = MONGO_HOST
        self.mongo_port = MONGO_PORT
        self.mongo_db = MONGO_DB
        self.mongo_collection = mongo_collection
        self.entity_code = entity_code
        self.client = pymongo.MongoClient(host=self.mongo_host, port=self.mongo_port, serverSelectionTimeoutMS=60,
                                          connectTimeoutMS=60, connect=False)

    def client_to_mongodb(self):
        db = self.client[self.mongo_db]
        try:
            collection_list = db.collection_names()
            return db, collection_list
        except pymongo.errors.ServerSelectionTimeoutError:
            for i in range(2, 6):
                try:
                    collection_list = db.collection_names()
                    return db, collection_list
                except pymongo.errors.ServerSelectionTimeoutError as e:
                    print("mongo{}:{}第{}次超时正在重新连接".format(self.mongo_host, self.mongo_port, i))
                if i == 5:
                    print("mongo第5次连接超时请检查")
                    self.client.close()

    def get_check_collection(self, db, collection_list):
        if self.mongo_collection in collection_list:
            collection = db[self.mongo_collection]
            return collection
        else:
            print("MongoDB没有该集合，请检查")
            self.client.close()

    def get_data_from_mongodb(self, collection, limitNumber=None):
        try:
            result_one = collection.find_one()
            if result_one:
                if limitNumber:
                    result = collection.find(no_cursor_timeout=True).limit(int(limitNumber))
                else:
                    result = collection.find(no_cursor_timeout=True)
                return result
            else:
                print("MONGO里面没有数据，请检查")
                self.client.close()
        except pymongo.errors.ServerSelectionTimeoutError:
            result_one = collection.find_one()
            if result_one:
                if limitNumber:
                    result = collection.find(no_cursor_timeout=True).limit(int(limitNumber))
                else:
                    result = collection.find(no_cursor_timeout=True)
                return result
            else:
                print("MONGO里面没有数据，请检查")
                self.client.close()

    # 根据ENTITY_CODE_查询, 返回游标对象
    def search_from_mongodb(self, collection, field_name="ENTITY_CODE_", field_value={"$exists": True}, data_id=None):
        if data_id:
            find_id = ObjectId(data_id)
            result_one = collection.find_one({"$and":
                        [{"ENTITY_CODE_": self.entity_code}, {"_id": {"$gte": find_id}}, {field_name: field_value}]})

        else:
            result_one = collection.find_one({"$and": [{"ENTITY_CODE_": self.entity_code}, {field_name: field_value}]})
        if result_one is not None:
            result = collection.find({"$and":
                [{"ENTITY_CODE_": self.entity_code}, {"_id": {"$gte": result_one["_id"]}}, {field_name: field_value}]},
                                     no_cursor_timeout=True)
            return result
        else:
            print("mongo中没有数据请检查")
            return None

    def remove_col_from_mongodb(self, collection, field_name="ENTITY_CODE_", field_value=None, query=None):
        if not query:
            count = collection.update({field_name: field_value}, {'$unset': {'d': ''}})
            # count = collection.update({'ENTITY_CODE_': 'CRMJPFX_XD_HXWYHXD'}, {'$unset': {'d': ''}}, False, True)
        return count

    def aggregate_from_mongodb(self, collection, field_name="ENTITY_CODE_", field_value=None, count_item='URL_'):
        result = collection.aggregate([{"$match":{field_name:field_value}}, {"$group":{"_id": f'${count_item}', "count":{"$sum":1}}}])
        return result

    def del_duplicate_data(self, collection, field_name="ENTITY_CODE_", field_value=None, count_item='URL_'):
        result = collection.aggregate(
            [{"$match": {field_name: field_value}}, {"$group": {"_id": f'${count_item}', "count": {"$sum": 1}}}])
        data_status = True
        while True:
            for i in result:
                print(i["count"])
                if i["count"] > 1:
                    data_status = False
                    print(i["_id"])
                    count = collection.delete_one({f'{count_item}': i["_id"]})
            if data_status:
                break
        return count

    def close_mongodb(self):
        self.client.close()


if __name__ == '__main__':
    # mongoclient = Mongoclient(entity_code="WD_SH_XX_51SXW", mongo_collection="WD_SS_XX")
    # db, collection_list = mongoclient.client_to_mongodb()
    # collection = mongoclient.get_check_collection(db, collection_list)
    # result = collection.aggregate([{"$match":{"ENTITY_CODE_":"WD_SH_XX_51SXW"}}, {"$group":{"_id":"$URL_", "count":{"$sum":1}}}])
    #
    # # result = mongoclient.aggregate_from_mongodb(collection=collection, field_value="ABCORGANIZE", count_item="NAME_")

    mongoclient = Mongoclient(entity_code="CRMJPFX_XD_HXWYHXD", mongo_collection="CRMJPFX_XD")
    db, collection_list = mongoclient.client_to_mongodb()
    collection = mongoclient.get_check_collection(db, collection_list)
    # result = mongoclient.search_from_mongodb(collection)
    # # for i in result:
    # #     print(i)
    count = mongoclient.remove_col_from_mongodb(collection, field_value="CRMJPFX_XD_HXWYHXD")
    print(count)
    mongoclient.close_mongodb()

