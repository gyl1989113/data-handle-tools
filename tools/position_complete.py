from __config import *
from database._mysql import MysqlClient
from tools.read_excel import *
from tools.web_api_of_baidu import get_lat_lng, get_area


def mysql_connect():
    mysql_config = {
        "host": MYSQL_HOST_41,
        "port": MYSQL_PORT_41,
        "database": MYSQL_DATABASE_41,
        "user": MYSQL_USER_41,
        "password": MYSQL_PASSWORD_41,
        "table": "cha_di_position"
    }

    mysql_client = MysqlClient(**mysql_config)
    connection = mysql_client.client_to_mysql()

    return mysql_client, connection


def data_from_mysql(mysql_client=None, mysql_connection=None):
    """
    查询地区编码和经纬度
    :param mysql_client:
    :param mysql_connection:
    :return:
    """
    if not mysql_client:
        mysql_client, mysql_connection = mysql_client()
    if not mysql_connection:
        mysql_client, mysql_connection = mysql_client()
    # 省级
    province_list = mysql_client.search_from_mysql(connection=mysql_connection, where_condition="PARENT_ is null")
    # 市级
    city_list = mysql_client.search_from_mysql(connection=mysql_connection,
                                               where_condition='CODE_ REGEXP "[0-9][0-9][0-9][0-9]00"')
    # 区县级
    area_list = mysql_client.search_from_mysql(connection=mysql_connection,
                                               where_condition='PARENT_ REGEXP "[0-9][0-9][0-9][0-9]00"')
    # 省直辖县级行政区划
    dir_area_list = mysql_client.search_from_mysql(connection=mysql_connection,
                                                   where_condition='CODE_ REGEXP "9[0-9][0-9][0-9]$"')
    # 银行编码
    mysql_client.mysql_table = "cha_bank"
    bank_list = mysql_client.search_from_mysql(connection=mysql_connection)
    # cls.mysql_client.close_client(connection=cls.mysql_connection)

    return province_list, city_list, area_list, dir_area_list, bank_list


if __name__ == '__main__':
    # mysql_client, connection = mysql_connect()
    # province_list, city_list, area_list, dir_area_list, bank_list = data_from_mysql(mysql_client, connection)
    data_list = list()
    workbook = read_excel_file(r"D:\wechat.xlsx")
    sheetname = workbook.sheet_names()[0]
    sheet = workbook.sheet_by_name(sheetname)
    row_list = sheet.row_values(0)
    for n in range(1, sheet.nrows):
        re_data = dict()
        data_item = dict()
        row_data = sheet.row_values(n)
        # 根据市名称找到省名称,省编码
        # for city in city_list:
        #     if city["NAME_"] == row_data[0]:
        #         row_data[7] = city["CODE_"]
        #         row_data[6] = city["PARENT_"]
        #         break
        # for prov in province_list:
        #     if prov["CODE_"] == row_data[6]:
        #         row_data[5] = prov["NAME_"]
        try:
            area_result = get_area(",".join([str(row_data[1]), str(row_data[2])]))
        except Exception as e:
            print(f"获取地址失败, ERROR: {e}")
        else:
            try:
                re_data["PROVINCE_NAME_"] = area_result["result"]["addressComponent"]["province"]
                re_data["CITY_NAME_"] = area_result["result"]["addressComponent"]["city"]
                re_data["AREA_NAME_"] = area_result["result"]["addressComponent"]["district"]
                re_data["AREA_CODE_"] = area_result["result"]["addressComponent"]["adcode"]
                re_data["CITY_CODE_"] = re_data["AREA_CODE_"][:4] + "00"
                re_data["PROVINCE_CODE_"] = re_data["AREA_CODE_"][:2] + "00"
            except KeyError:
                pass
        row_data[5] = re_data["PROVINCE_NAME_"]
        row_data[6] = re_data["PROVINCE_CODE_"]
        row_data[7] = re_data["CITY_CODE_"]
        data_item = dict(zip(row_list, row_data))
        to_csv(data_item)
