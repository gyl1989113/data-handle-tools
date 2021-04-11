import pymysql
from __config import *
from retrying import retry

class MysqlClient(object):
    def __init__(self, entity_code=None, **kwargs):
        # if not kwargs:
        #     self.mysql_host = MYSQL_HOST_41
        #     self.mysql_port = MYSQL_PORT_41
        #     self.mysql_database = MYSQL_DATABASE_41
        #     self.mysql_table = MYSQL_TABLE_41
        #     self.mysql_user = MYSQL_USER_41
        #     self.mysql_password = MYSQL_PASSWORD_41
        if not kwargs:
            self.mysql_host = MYSQL_HOST_LOCAL
            self.mysql_port = MYSQL_PORT_LOCAL
            self.mysql_database = MYSQL_DATABASE_LOCAL
            self.mysql_table = MYSQL_TABLE_LOCAL
            self.mysql_user = MYSQL_USER_LOCAL
            self.mysql_password = MYSQL_PASSWORD_LOCAL
        else:
            self.mysql_host = kwargs["host"]
            self.mysql_port = kwargs["port"]
            self.mysql_database = kwargs["database"]
            self.mysql_table = kwargs["table"]
            self.mysql_user = kwargs["user"]
            self.mysql_password = kwargs["password"]

        self.entity_code = entity_code
        self.mysql_config = {
            "host": self.mysql_host,
            "port": self.mysql_port,
            "database": self.mysql_database,
            "user": self.mysql_user,
            "passwd": self.mysql_password,
            "charset": "utf8"
        }

    def client_to_mysql(self):
        try:
            connection = pymysql.connect(**self.mysql_config)
            print("正在连接mysql{} {} {}".format(self.mysql_host, self.mysql_port, self.mysql_database))
            return connection
        except pymysql.err.OperationalError as e:
            for retry_count in range(2, 7):
                try:
                    connection = pymysql.connect(**self.mysql_config)
                    print("第{}次连接mysql{} {} {}".format(retry_count, self.mysql_host, self.mysql_port, self.mysql_database))
                    return connection
                except Exception as e:
                    print("第{}次连接mysql失败".format(retry_count))
                    if retry_count >=6:
                        print("MYSQL连接失败错误信息为请检查配置")

    @staticmethod
    def cs_commit(connection, sql):
        cs = connection.cursor()
        count = cs.execute(sql)
        connection.commit()
        cs.close
        return count

    def insert_to_mysql(self, connection, data):
        if isinstance(data, dict):
            k_list = [key for key in data.keys()]
            v_list = tuple([value for value in data.values()])
            v_sql = str(v_list)
            if v_sql[-2] == ",":
                v_sql = v_sql[:-2] + ")"
            sql = f'insert into {self.mysql_table} ({",".join(k_list)}) values{v_sql}'
        elif isinstance(data, (tuple, list)):
            k_list = [key for key in data[0].keys()]
            value_list = list()

            for each in data:
                v_list = str(tuple([value for value in each.values()]))
                if v_list[-2] == ",":
                    v_list = v_list[:-2] + ")"
                value_list.append(v_list)
            sql = f'insert into {self.mysql_table} ({",".join(k_list)}) values'
            sql = sql + ",".join(value_list)
        else:
            raise Exception("not format data type")
        try:
            print(sql)
            count = self.cs_commit(connection=connection, sql=sql)
            print("插入成功{}条".format(count))
        except Exception as e:
            print(f'MySQL 插入失败，ERROR: {e}')

    def update_to_mysql(self, connection, data, where_condition):
        set_list = list()
        for key, value in data.items():
            set_list.append(f"{key}=\'{value}\'")
        if "where" in where_condition:
            sql = f'update {self.mysql_table} set {",".join(set_list)} {where_condition}'

        else:
            sql = f'update {self.mysql_table} set {",".join(set_list)} where {where_condition}'
        try:
            count = self.cs_commit(connection=connection, sql=sql)
            print("修改成功{}条".format(count))
        except Exception as e:
            print("f'MySQL 插入失败，ERROR: {e}'")

    def search_from_mysql(self, connection, output=None, where_condition=None, limit_num=None, offset_num=None):
        """
        查询
        :param connection:
        :param output: 输出字段
        :param where_condition: where 条件
        :param limit_num: 输出数量
        :param offset_num: 跳过数量
        :return:
        """
        if output:
            if isinstance(output, str):
                sql = f"SELECT {output} FROM {self.mysql_table}"
            elif isinstance(output, (tuple, list)):
                sql = f"SELECT {','.join(output)} FROM {self.mysql_table}"
            else:
                raise Exception("not format type of \"output\"")
        else:
            sql = f"SELECT * FROM {self.mysql_table}"

        if where_condition:
            if "where" in where_condition or "WHERE" in where_condition:
                sql = sql + " " + where_condition
            else:
                sql = sql + f" WHERE {where_condition}"

        sql = sql + f" LIMIT {limit_num}" if limit_num else sql

        sql = sql + f" OFFSET {offset_num}" if offset_num else sql

        try:
            # 以字典方式查询出结果
            cs = connection.cursor(pymysql.cursors.DictCursor)
            count = cs.execute(sql)
            result = cs.fetchall()
            if count:
                print(f"Mysql 查取成功 {count} 条")
                return result
            else:
                print("数据库查取数为0")
        except TypeError:
            print("MySQL查取失败，请检查")
        finally:
            cs.close()

    def get_table_column(self, connection, primary_key="N", table_name=None):
        if not table_name:
            table_name = self.mysql_table
        if primary_key == "N":
            sql = f"SELECT column_name FROM information_schema.columns WHERE table_name=\'{table_name}\';"
        else:
            sql = f"SELECT column_name FROM information_schema.columns WHERE table_name=\'{table_name}\' AND constraint_name='PRIMARY';"
        curs = connection.cursor()
        curs.execute(sql)
        column_list = curs.fetchall()
        # print(column_list)
        _column_list = [column[0] for column in column_list]
        # print(_column_list)
        return _column_list

    def duplicate_data_remove(self, connection, column, table_name=None, value=None):
        if not table_name:
            table_name = self.mysql_table
        if not value:
            sql = f'select {column},count(1) from {table_name} group by {column} having count(1)>1'
        else:
            where_condition = column + "='" + value + "'"
            sql = f'select {column},count(1) from {table_name} where ' \
                  f'{where_condition} group by {column} having count(1)>1'
        # print(sql)
        curs = connection.cursor()
        curs.execute(sql)
        result = curs.fetchall()
        # print(count)
        dupli_tag = False
        for dupli_data in result:
            # 打印重复数据
            print(int(str(dupli_data[1])))
            print(str(dupli_data[0]))
            if int(str(dupli_data[1])) > 1:
                dupli_tag = True
                break
        if dupli_tag:
            # remove_sql = f'delete from {table_name} where {column} = \'{str(dupli_data[0])}\''
            remove_sql = f'delete {table_name} from {table_name},(select min(ID) id,{column} from ' \
                         f'{table_name} group by {column} having count(*)>1) t2 where ' \
                         f'{table_name}.{column} = t2.{column} and {table_name}.ID > t2.id'
            print(remove_sql)
            count = curs.execute(remove_sql)
            connection.commit()
            print(count)
        curs.close()


if __name__ == '__main__':
    mysql_client = MysqlClient()
    connection = mysql_client.client_to_mysql()
    data1 = {'commenttime': '2019-03-27', 'comment': 'fuck you'}
    data2 = [{'commenttime': '2019-03-29', 'comment': 'fuck you and me'}, {'commenttime': '2019-03-28', 'comment': 'fuck me'}]
    data3 = {'commenttime': '2019-03-29', 'comment': 'fuck me and you'}
    # mysql_client.insert_to_mysql(connection=connection, data=data3)
    # mysql_client.update_to_mysql(connection=connection, data=date3, where_condition="where commenttime='2019-03-29'")
    # mysql_client.duplicate_data_remove(connection=connection, table_name="huoguodian", column="comment", value="fuck you")
    # mysql_client.duplicate_data_remove(connection=connection, column="comment")
    mysql_client.get_table_column(connection=connection)