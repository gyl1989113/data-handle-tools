# -*- coding: utf-8 -*-
import os
import sys
curPath = os.path.abspath(os.path.dirname(__file__))
sys.path.append(curPath[:-9])
import hashlib
import jpype
import jaydebeapi
import re
from hbase import Hbase
from hbase.ttypes import ColumnDescriptor, BatchMutation, Mutation
from thrift.protocol import TCompactProtocol, TBinaryProtocol
from thrift.transport import TSocket, TTransport
from thrift.transport.TTransport import TFramedTransport
from __config import HBASE_HOST, HBASE_PORT, HBASE_COLUM_FAMILY
from database._mongodb import MongoClient


curPath = os.path.abspath(os.path.dirname(__file__))
src_dir = curPath[:-9] + "/jars"


class ThriftHbase(object):
    def __init__(self):
        __socket = TSocket.TSocket(host=HBASE_HOST, port=HBASE_PORT)
        __socket.setTimeout(50000)
        self.transport = TFramedTransport(__socket)
        # transport = TTransport.TBufferedTransport(socket)
        __protocol = TCompactProtocol.TCompactProtocol(self.transport)
        # protocol = TBinaryProtocol.TBinaryProtocol(transport)
        self.client = Hbase.Client(__protocol)
        self.transport.open()

    def create_table_hbase(self, table_name, column_list=None):
        column_families = list()
        # 定义列族
        if column_list:
            for each in column_list:
                column = ColumnDescriptor(name=each)
                column_families.append(column)
        else:
            column = ColumnDescriptor(name=HBASE_COLUM_FAMILY)
            column_families.append(column)
        # 创建表
        self.client.createTable(table_name, column_families)

    def delete_table_hbase(self, table_name):
        self.client.disableTable(table_name)
        self.client.deleteTable(table_name)

    def insert_row_into_hbase(self, table_name, data):
        mutation_list = list()
        row_key = str(data["_id"])
        # del data["_id"]
        if "d" in data:
            del data["d"]
        for key, value in data.items():
            if key == "_id":
                continue
            else:
                mutation = Mutation(column="{}:{}".format(HBASE_COLUM_FAMILY, str(key)), value=str(value))
                mutation_list.append(mutation)

        self.client.mutateRow(table_name, row_key, mutation_list)

        # batch_mutation1 = BatchMutation("ROW1", mutation_list)
        # batch_mutation2 = BatchMutation("ROW1", mutation_list)
        # batch_mutation_list = [batch_mutation1, batch_mutation2]
        # client.mutateRows(table_name, batch_mutation_list)

    def delete_row_from_hbase(self, table_name, row):
        self.client.deleteAllRow(table_name, row)

    def get_row_from_hbase(self, table_name, row):
        return self.client.getRow(table_name, row)


class PhoenixHbase(object):
    def __init__(self, table_name):
        # print(curPath)
        # print(src_dir)
        self.phoenix_client_jar = src_dir + "/phoenix-5.0.0-HBase-2.0-client.jar"
        args = '-Djava.class.path=%s' % self.phoenix_client_jar
        jvm_path = jpype.getDefaultJVMPath()
        # print(args)
        jpype.startJVM(jvm_path, args)
        self.count = 0
        self.table_name = table_name
        self.position_dict = dict()
        # 字段验证列表
        self.verify_list = ["ID_", "CONTENT_", "NOTICE_TIME_", "TITLE_", "PROJECT_NAME_", "BID_CONTENT_",
                            "SIGN_START_TIME_", "SIGN_END_TIME_", "OPEN_BID_TIME_", "OPEN_BID_PLACE_", "BID_AGENCY_",
                            "APPLY_CONDITION_", "SIGN_QUALIFICATION_", "PROJECT_ID_", "WIN_CANDIDATE_",
                            "CANDIDATE_RANK_", "BID_", "URL_", "ENTITY_NAME_", "ENTITY_CODE_",
                            "ENTITY_STATUS_", "SIGN_MATERIAL_", "BID_TYPE_", "DATETIME_", "BUDGET_PRICE_",
                            "PASS_REASON_", "PRESALE_CONTENT_", "PRESALE_WAY_", "PRESALE_START_TIME_",
                            "PRESALE_END_TIME_", "PRESALE_ADDR_", "PRESALE_PREPARE_", "IMAGE_"]

    def __connect_to_phoenix(self):
        connection = jaydebeapi.connect('org.apache.phoenix.jdbc.PhoenixDriver',
                                        'jdbc:phoenix:storage01,storage02,storage03:2181:/hbase',
                                        ['', ''],
                                        self.phoenix_client_jar)
        return connection

    def connect_to_phoenix(self):
        # connection = jaydebeapi.connect('org.apache.phoenix.jdbc.PhoenixDriver',
        #                                 'jdbc:phoenix:storage01,storage02,storage03:2181:/hbase',
        #                                 ['', ''],
        #                                 self.phoenix_client_jar)
        # conn = jaydebeapi.connect('oracle.jdbc.driver.OracleDriver',
        #                           ['jdbc:oracle:thin:@127.0.0.1/orcl', 'scott', 'tiger'], 'D:\\MY_TOOLS\\ojdbc6.jar')

        connection = jaydebeapi.connect('org.apache.phoenix.jdbc.PhoenixDriver',
                                        'jdbc:phoenix:storage01,storage02,storage03:2181:/hbase',
                                        {"user": '', "password": ""},
                                        self.phoenix_client_jar, {"hbase.client.keyvalue.maxsize": "52428800"})
        return connection

    # todo only full data is true
    def __search_iter(self, curs):
        while True:
            result = curs.fetchone()

            if self.position_dict:
                if result:
                    result_dict = dict()
                    for j in range(len(result)):
                        result_dict[self.position_dict[j + 1]] = result[j]
                    yield result_dict
                else:
                    curs.close()
                    raise StopIteration("No item to fetch, all item is already done")
            else:
                if result:
                    yield result
                else:
                    curs.close()
                    raise StopIteration("No item to fetch, all item is already done")

    def __get_coloumn(self, connection, table_name):
        curs = connection.cursor()
        curs.execute(
            f"SELECT ORDINAL_POSITION, COLUMN_NAME FROM SYSTEM.\"CATALOG\" WHERE TABLE_NAME = \'{table_name}\'")
        position_list = curs.fetchall()
        curs.close()

        for each in position_list:
            self.position_dict[each[0]] = each[1]

    def search_all_from_phoenix(self, connection, table_name=None, output_field=None, limit_num=None, offset_num=None,
                                iter_status=True, dict_status=False, where_condition=None):
        """
        查询
        :param connection: phoenix 连接对象
        :param output_field: 输出值的字段，类型为 str 或 list
        :param limit_num: 输出条数
        :param offset_num: 跨度，如跳过前10条数据
        :param iter_status: 为 True 返回值为生成器对象; 为 False 返回值为数据列表(推荐数据量较小时或 limit_num 较小时用)
        :param dict_status: True 返回字典类型数据, 键为字段, 值为字段对应的值
        :param where_condition: where 条件 exp: "WHERE ENTITY_CODE_ = 'XXX'" , 等号后面的值必须为单引号
        :return: 生成器对象 或 数据列表
        """

        if not table_name:
            table_name = self.table_name

        if dict_status:
            if not self.position_dict:
                self.__get_coloumn(connection=connection, table_name=table_name)

        if not output_field:
            sql = f"SELECT * FROM \"{table_name}\""
        elif isinstance(output_field, str):
            sql = f"SELECT \"{output_field}\" FROM \"{table_name}\""
        elif isinstance(output_field, list):
            sql = f"SELECT {','.join(output_field)} FROM \"{table_name}\""
        else:
            return "param \"output_field\" need str or list"

        if where_condition:
            if ("where" in where_condition) or ("WHERE" in where_condition):
                where_condition = where_condition.replace("where", "")
                where_condition = where_condition.replace("WHERE", "")
            sql = f"{sql} WHERE {where_condition}"

        if limit_num:
            sql = sql + f" LIMIT {limit_num}"

        if offset_num:
            sql = sql + f" OFFSET {offset_num}"

        curs = connection.cursor()
        curs.execute(sql)

        if iter_status:
            return self.__search_iter(curs=curs)
        else:
            result = list()
            one_result = self.__search_iter(curs=curs)
            while True:
                try:
                    result.append(one_result.__next__())
                except StopIteration:
                    return result

    def copy_table_phoenix(self, connection, source_table, new_table, modify_dict=None, create_status=False):
        """
        复制数据到新的表中
        :param connection: Phoenix 连接对象
        :param source_table: 数据来源的表名
        :param new_table: 数据去向的表名
        :param create_status: 是否创建 new_table 表, 默认为否
        :return: None
        """
        if create_status:
            self.__get_coloumn(connection=connection, table_name=source_table)

            column_arrary_list = list()
            for i in range(1, len(self.position_dict)):
                if self.position_dict[i] == "CONTENT_":
                    column_arrary_list.append(("T", test1_.position_dict[i]))
                else:
                    column_arrary_list.append(("C", test1_.position_dict[i]))

            self.create_table_phoenix(connection=connection, column_arrary_list=column_arrary_list,
                                      table_name=new_table)

        result_generator = self.search_all_from_phoenix(connection=connection, dict_status=True)

        self.table_name = new_table

        while True:
            try:
                result = result_generator.__next__()
                if modify_dict:
                    result.update(modify_dict)
            except StopIteration:
                break

            test1_.upsert_to_phoenix_by_one(connection=connection, data=result)

    def drop_table_phoenix(self, connection, table_name=None):
        """
        删除已存在的表
        :param connection: Phoenix 连接对象
        :param table_name: 需删除的表名
        :return: None
        """
        # ALTER TABLE CHA_BRANCH_WECHAT DROP COLUMN IMAGE_
        pho_logger = Logger().logger

        if not table_name:
            table_name = self.table_name
        curs = connection.cursor()
        curs.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        pho_logger.info(f"表 {table_name} 删除成功")
        connection.commit()
        curs.close()

    def delete_from_phoenix(self, connection, where_condition=None):
        pho_logger = Logger().logger

        sql = f"DELETE FROM {self.table_name}"

        if where_condition:
            sql = sql + f" WHERE {where_condition}"
        curs = connection.cursor()
        curs.execute(sql)
        pho_logger.info(f"数据删除成功")
        connection.commit()
        curs.close()

    def create_new_table_phoenix(self, connection, sql):
        pho_logger = Logger().logger
        curse = connection.cursor()
        curse.execute(sql)
        connection.commit()
        pho_logger.info("{} 表创建成功".format(self.table_name))
        curse.close()

    def create_table_phoenix(self, connection, column_arrary_list, table_name=None):
        """
        创建表
        :param connection: Phoenix 连接对象
        :param column_arrary_list: 要创建表的 列族名,列名 组成的列表 exp: [("C", "ID_"), ("C", "ENTITY_CODE_")]
        :param table_name: 要创建表的表名, 默认为创建 PhoenixHbase 对象时传入的表名
        :return: None
        """
        pho_logger = Logger().logger

        if not table_name:
            table_name = self.table_name

        column_sql_list = list()
        for each_column in column_arrary_list:
            if each_column[1] == "ID_":
                continue
            column_sql = f'"{each_column[0]}"."{each_column[1]}" VARCHAR'
            column_sql_list.append(column_sql)

        sql = f'CREATE TABLE "{table_name}" ("ID_" varchar primary key,{",".join(column_sql_list)})IMMUTABLE_ROWS = true'

        curse = connection.cursor()
        curse.execute(sql)
        connection.commit()
        pho_logger.info("{} 表创建成功".format(self.table_name))
        curse.close()

    def add_column_phoenix(self, connection, column_arrary_list, table_name=None):
        """
        为已存在的表添加新的列族和列
        :param connection: Phoenix 连接对象
        :param column_arrary_list: 需添加的 列族名,列名 组成的列表 exp: [("C", "ID_"), ("C", "ENTITY_CODE_")]
        :param table_name: 需添加列的表名, 默认为创建 PhoenixHbase 对象时传入的表名
        :return:
        """
        pho_logger = Logger().logger

        if not table_name:
            table_name = self.table_name

        column_sql_list = list()
        for each_column in column_arrary_list:
            column_sql = f'"{each_column[0]}"."{each_column[1]}" VARCHAR'
            column_sql_list.append(column_sql)

        sql = f'ALTER TABLE "{table_name}" ADD {",".join(column_sql_list)}'

        curs = connection.cursor()
        curs.execute(sql)
        connection.commit()
        pho_logger.info(f"表 {table_name} 添加列 {str(column_arrary_list)[1:-1]} 成功")
        curs.close()

    def upsert_to_phoenix(self, connection, data_list):
        pho_logger = Logger().logger
        self.count = 0
        for data in data_list:
            sql = None
            batch_key = list()
            batch_value_ = list()
            batch_value = list()
            if self.table_name == "CommonBidding":
                if ("entityStatus" not in data) and ("ENTITY_STATUS_" not in data):
                    data["ENTITY_STATUS_"] = "DRAFT"
                if "url" in data and "URL_" in data:
                    data.pop("url")
                if "winningCandidate" in data:
                    data["WIN_CANDIDATE_"] = data.pop("winningCandidate")
                if "word" in data:
                    data["CONTENT_"] = data.pop("word")
                if "biddingAgency" in data:
                    data["BID_AGENCY_"] = data.pop("biddingAgency")
                if "singupStartTime" in data:
                    data["SIGN_START_TIME_"] = data.pop("singupStartTime")
                if "singupEndTime" in data:
                    data["SIGN_END_TIME_"] = data.pop("singupEndTime")
                if "singupEndTime " in data:
                    data["SIGN_END_TIME_"] = data.pop("singupEndTime ")
                if "singupMaterial" in data:
                    data["SIGN_MATERIAL_"] = data.pop("singupMaterial")
                if "singupQualification" in data:
                    data["SIGN_QUALIFICATION_"] = data.pop("singupQualification")
                if "singupEndTime" in data:
                    data["SIGN_QUALIFICATION_"] = data.pop("singupEndTime")
                if "singupCondition" in data:
                    data["APPLY_CONDITION_"] = data.pop("singupCondition")
                if "bigText" in data:
                    data["CONTENT_"] = data.pop("bigText")
                if "candidateRanking" in data:
                    data.pop("candidateRanking")
                if "quote" in data:
                    data.pop("quote")

            if "_id" not in data:
                value_t = ""
                if "TITLE_" in data:
                    hash_m = hashlib.md5()
                    hash_m.update(str(data["TITLE_"]).encode("utf-8"))
                    hash_title = hash_m.hexdigest()
                    # if "NOTICE_TIME_" in data:
                    #     value_t = str(data["ENTITY_CODE_"]) + "_" + str(hash_title) + "_" + str(data["NOTICE_TIME_"])
                    # else:
                    #     value_t = str(data["ENTITY_CODE_"]) + "_" + str(hash_title)
                    value_t = str(data["ENTITY_CODE_"]) + "_" + str(hash_title)
                elif "title" in data:
                    hash_m = hashlib.md5()
                    hash_m.update(data["title"].encode("utf-8"))
                    hash_title = hash_m.hexdigest()
                    value_t = str(data["entity_code"]) + "_" + str(hash_title)
                data["ID_"] = value_t

            for key, value in data.items():
                # 字段验证
                if key == "_id":
                    key = "ID_"
                    if "TITLE_" in data:
                        hash_m = hashlib.md5()
                        hash_m.update(str(data["TITLE_"]).encode("utf-8"))
                        hash_title = hash_m.hexdigest()
                        if "NOTICE_TIME_" in data:
                            value = str(data["ENTITY_CODE_"]) + "_" + str(hash_title) + "_" + str(data["NOTICE_TIME_"])
                        else:
                            value = str(data["ENTITY_CODE_"]) + "_" + str(hash_title)

                    elif "title" in data:
                        hash_m = hashlib.md5()
                        hash_m.update(data["title"].encode("utf-8"))
                        hash_title = hash_m.hexdigest()

                        value = str(data["entity_code"]) + "_" + str(hash_title)

                if key == "entityStatus" or key == "ENTITY_STATUS_":
                    key = "ENTITY_STATUS_"
                    value = "DRAFT"

                # if key not in self.verify_list:
                #     for verify in self.verify_list:
                #         key_v = key.replace("_", "").upper()
                #         if key_v == verify.replace("_", ""):
                #             key = verify

                value = str(value).replace("[", "(")
                # print(value)
                value = value.replace("]", ")")
                value = value.replace("\'", "\"")

                batch_key.append(key)
                batch_value.append(value)

            batch_key_ = str(batch_key).replace("[", "(")
            batch_key_ = batch_key_.replace("]", ")")
            batch_key_ = batch_key_.replace("'", "")
            sql = 'upsert into \"{}\" {} values(?{})'.format(self.table_name, batch_key_, (",?" * (len(batch_key) - 1)))
            # print(sql)
            batch_value_.append(batch_value)

            curs = connection.cursor()
            curs.setinputsizes(52428800)
            # print(batch_value_)
            # print(len(batch_value))
            curs.executemany(sql, batch_value_)
            connection.commit()
            # print("插入成功")
            self.count += 1
            curs.close()

        pho_logger.info("插入成功{}条".format(self.count))

        return self.count

    # 插入单条
    def upsert_to_phoenix_by_one(self, connection, data):
        # self.count = 0

        sql = None
        batch_key = list()
        batch_value_ = list()
        batch_value = list()
        if self.table_name == "CommonBidding" or self.table_name == "CommonBidding_test":
            if data["CONTENT_"]:
                data["CONTENT_"] = data["CONTENT_"].replace("|", "")
            if data["TITLE_"]:
                if isinstance(data["TITLE_"], list):
                    data["TITLE_"] = data["TITLE_"][0]
            # print(data["TITLE_"])
            # print(type(data["TITLE_"]))
            # data["TITLE_"] = data["TITLE_"]
            if ("url" in data) and ("URL_" in data):
                data.pop("url")
            if "winningCandidate" in data:
                data["WIN_CANDIDATE_"] = data.pop("winningCandidate")
            if "word" in data:
                data["CONTENT_"] = data.pop("word")
            if "biddingAgency" in data:
                data["BID_AGENCY_"] = data.pop("biddingAgency")
            if "singupStartTime" in data:
                data["SIGN_START_TIME_"] = data.pop("singupStartTime")
            if "singupEndTime" in data:
                data["SIGN_END_TIME_"] = data.pop("singupEndTime")
            if "singupEndTime " in data:
                data["SIGN_END_TIME_"] = data.pop("singupEndTime ")
            if "singupMaterial" in data:
                data["SIGN_MATERIAL_"] = data.pop("singupMaterial")
            if "singupQualification" in data:
                data["SIGN_QUALIFICATION_"] = data.pop("singupQualification")
            if "singupEndTime" in data:
                data["SIGN_QUALIFICATION_"] = data.pop("singupEndTime")
            if "singupCondition" in data:
                data["APPLY_CONDITION_"] = data.pop("singupCondition")
            if "bigText" in data:
                data["CONTENT_"] = data.pop("bigText")
            if "candidateRanking" in data:
                data.pop("candidateRanking")
            if "quote" in data:
                data.pop("quote")
            if ("entityStatus" not in data) and ("ENTITY_STATUS_" not in data) and\
                    ("STATUS_1" not in data) and ("STATUS_" not in data):
                data["ENTITY_STATUS_"] = "DRAFT"

        if ("_id" not in data) and ("ID_" not in data):
            # 根据 "URL_" 定义 row_key
            if "URL_" in data:
                hash_m = hashlib.md5()
                hash_m.update(str(data["URL_"]).encode("utf-8"))
                hash_title = hash_m.hexdigest()
                value_t = str(data["ENTITY_CODE_"]) + "_" + str(hash_title)
            elif "DETAILED_URL_" in data:
                hash_m = hashlib.md5()
                hash_m.update(str(data["DETAILED_URL_"]).encode("utf-8"))
                hash_title = hash_m.hexdigest()
                value_t = str(data["ENTITY_CODE_"]) + "_" + str(hash_title)
            else:
                hash_m = hashlib.md5()
                hash_m.update(str(data["TITLE_"]).encode("utf-8"))
                hash_title = hash_m.hexdigest()
                value_t = str(data["ENTITY_CODE_"]) + "_" + str(hash_title)

            data["ID_"] = value_t

        for key, value in data.items():
            # if key not in self.verify_list:
            #     for verify in self.verify_list:
            #         key_v = key.replace("_", "").upper()
            #         if key_v == verify.replace("_", ""):
            #             key = verify

            if (value == "None") or (value is None):
                value = ""
            value = str(value).replace("[", "(")
            value = re.sub(r"[^~·`!！@#$￥%^…&*()（）_—\-+={}\[\]【】、:：；;\'\"‘’“”<>《》,，.。/?？| \w]", "", value)
            value = value.replace("]", ")")
            value = value.replace("\'", "\"")

            batch_key.append(key)
            batch_value.append(value)

        batch_key_ = str(batch_key).replace("[", "(")
        batch_key_ = batch_key_.replace("]", ")")
        batch_key_ = batch_key_.replace("\'", "")
        # print(data)
        sql = 'upsert into \"{}\" {} values(?{})'.format(self.table_name, batch_key_, (",?" * (len(batch_key) - 1)))
        # print(sql)
        batch_value_.append(batch_value)
        # print(batch_value)
        curs = connection.cursor()
        curs.setinputsizes(52428800)
        curs.setoutputsize(52428800)
        # print(batch_value_)
        # print(len(batch_value))
        curs.executemany(sql, batch_value_)
        # print(data["ID_"])
        connection.commit()
        # print("插入成功")
        self.count += 1
        curs.close()

        # pho_logger.info("插入成功{}条".format(self.count))

        return self.count

    def close_client_phoenix(self, connection):
        connection.close()


    # def jz_upsert_to_phoenix(self, connection, data_list, key_list):
    #     pho_logger = Logger().logger
    #     self.count = 0
    #     batch_value_ = list()
    #     for data in data_list:
    #         sql = None
    #         batch_value = list()
    #
    #         for key in key_list:
    #             # 字段验证
    #             value = data[key]
    #             value = str(value).replace("[", "(")
    #             # print(value)
    #             value = value.replace("]", ")")
    #             value = value.replace("\'", "\"")
    #
    #             batch_value.append(value)
    #         batch_value_.append(batch_value)
    #     batch_key_ = str(key_list).replace("[", "(")
    #     batch_key_ = batch_key_.replace("]", ")")
    #     batch_key_ = batch_key_.replace("'", "")
    #     sql = 'upsert into \"{}\" {} values(?{})'.format(self.table_name, batch_key_, (",?" * (len(key_list) - 1)))
    #     curs = connection.cursor()
    #     curs.setinputsizes(52428800)
    #     # print(batch_value_)
    #     # print(len(batch_value))
    #     curs.executemany(sql, batch_value_)
    #     connection.commit()
    #     # print("插入成功")
    #
    #     curs.close()
    #
    #     return True

    def change_error_commonbidding(self, connection):

        self.table_name = "CommonBidding"
        # test1_ = PhoenixHbase(table_name="CHA_BRANCH_INSURANCE")
        result_generator = self.search_all_from_phoenix(connection=connection, dict_status=True,
                                                        where_condition="ENTITY_STATUS_ = 'ERROR'")
        while True:
            try:
                result = result_generator.__next__()
                result["ENTITY_STATUS_"] = "DRAFT"
            except StopIteration:
                break

            self.upsert_to_phoenix_by_one(connection=connection, data=result)
        connection.close()


if __name__ == '__main__':
    test1_ = PhoenixHbase(table_name="CHA_BRANCH_WEIBO_INFO")
    # test1_ = PhoenixHbase(table_name="CHA_BRANCH_INSURANCE")
    connection = test1_.connect_to_phoenix()
    # m_client = MongoClient()
    # db = m_client.client["spider_data"]
    # collection = db["ZX_HYBG"]
    # m_result_gen = collection.find({"ENTITY_CODE_": "ZX_HYBG_ARZX_CYYJBG"})

    # # 复制数据到新的表
    # test1_.copy_table_phoenix(connection=connection, source_table="CommonBidding", new_table="CommonBidding_test")

    # # 删除表
    # test1_.drop_table_phoenix(connection=connection, table_name="ORGANIZE_FINASSIST_TEST")

    # # 创建表
    # column_arrary_list = [("C", "BANK_NAME_"), ("C", "BANK_CODE_"), ("C", "NAME_"), ("C", "CODE_"),
    #                       ("C", "ENTITY_NAME_"), ("C", "ENTITY_CODE_"), ("C", "AREA_CODE_"), ("C", "UNIT_CODE_"),
    #                       ("C", "ADDR_"), ("C", "PROVINCE_NAME_"), ("C", "PROVINCE_CODE_"), ("C", "CITY_"),
    #                       ("C", "CITY_CODE_"), ("C", "DISTRICT_NAME_"), ("C", "DISTRICT_CODE_"), ("C", "LAT_"),
    #                       ("C", "LNG_"), ("C", "CREATE_TIME_"), ("C", "DEALTIME_"), ("C", "URL_"), ("C", "TEL_"),
    #                       ("C", "BUSINESS_HOURS_"), ("C", "STATUS_"), ("C", "IMPORTANCE_")]
    #
    # test1_.create_table_phoenix(connection=connection, column_arrary_list=column_arrary_list,
    #                             table_name="MARKETING_ACT")

    # # 查询
    # # 返回列表
    # result = test1_.search_all_from_phoenix(connection=connection, iter_status=False, dict_status=True)

    # 返回生成器对象
    # result_generator = test1_.search_all_from_phoenix(connection=connection, dict_status=True,
    #                                                   where_condition="where ENTITY_CODE_ = 'MAPBAR_DEATAIL_BJ'")
    result_generator = test1_.search_all_from_phoenix(connection=connection, dict_status=False)
    count = 0
    field_list=[
        'ID_',
        'ENTITY_CODE_',
        'ENTITY_NAME_',
        'URL_',
        'PROVINCE_CODE_',
        'PROVINCE_NAME_',
        'CITY_CODE_',
        'CITY_NAME_',
        'AREA_CODE_',
        'AREA_NAME_',
        'LAT_',
        'LNG_',
        'BANK_CODE_',
        'BANK_NAME_',
        'UNIT_CODE_',
        'UNIT_NAME_',
        'PERIOD_CODE_',
        'REMARK_',
        'CREATE_TIME_',
        'SPIDER_TIME_',
        'MODIFIED_TIME_',
        'CREATE_BY_ID_',
        'CREATE_BY_NAME_',
        'MODIFIED_BY_ID_',
        'MODIFIED_BY_NAME_',
        'M_STATUS_',
        'DELETE_STATUS_',
        'DATA_STATUS_',
        'TAGS_',
        'SOURCE_',
        'SOURCE_NAME_',
        'SOURCE_TYPE_',
        'HOT_',
        'BRIEF_',
        'LIKES_',
        'COMMENTS_',
        'RELAYS_',
        'IMPORTANCE_',
        'PUBLISH_TIME_',
        'CONTENT_',
        'TYPE_',
        'PUBLISH_STATUS_',
        'TYPE_CODE_',
        'OWN_',
        'WEIBO_CODE_',
        'WEIBO_NAME_',
        'SENSITIVE_WORD_',
        'SENSITIVE_',
        'VERSION_',
        'IMAGE_1',
        'IMAGE_2',
        'IMAGE_3',
        'IMAGE_4',
        'IMAGE_5',
        'IMAGE_6',
        'IMAGE_7',
        'IMAGE_8',
        'IMAGE_9',
        'DATA_VERSION_',
        'DEAL_USER_',
        'DEAL_TIME_',
        'P_USER_',
        'P_TIME_',
        'DEAL_USER_ID_',
        'P_USER_ID_'
    ]

    while True:
        try:
            result = result_generator.__next__()
            data = dict()
            count_index = 0
            for index, value in enumerate(result):
                # print(index, field_list[index], value)
                # if index in [12]:
                data[field_list[index]] = result[index]
                if index >= 12:
                    try:
                        data[field_list[index]] = result[index+1]
                    except Exception:
                        break
                if index >= 44:
                    try:
                        data[field_list[index]] = result[index+3]
                    except Exception:
                        break
            # print(data)
        except StopIteration:
            break
        test1_.table_name = "TEST_CHA_BRANCH_WEIBO_INFO"
        test1_.upsert_to_phoenix_by_one(connection=connection, data=data)
        count += 1
        # print(count)
    connection.close()
