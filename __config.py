# -*- coding: utf-8 -*-
"""配置文件"""

"MongoDB 默认配置"
# MongoDB 地址

MONGO_HOST_LH = "localhost"
MONGO_HOST = "172.22.xx.xx"


# MongoDB 端口(int)
MONGO_PORT_20 = 20000
MONGO_PORT_27 = 27017

# MongoDB 数据库(str)
MONGO_DB = "spider_data"

# HBase 默认配置
# HBase 地址(str)
HBASE_HOST = "172.22.xx.xx"
# thrift 端口(int)
HBASE_PORT = 9090
# HBase 表名(str)
# HBASE_TABLE = "spider_data_test3"
# HBase 列族名(str)
HBASE_COLUM_FAMILY = "C"

"MySQL 默认配置"
# MySQL 地址(str)
MYSQL_HOST = "172.22.xx.xx"
# MySQL 端口(int)
MYSQL_PORT = 3306
# MySQL 数据库(str)
MYSQL_DATABASE = "spider"
# MySQL 表名(str)
MYSQL_TABLE = "spi_scra_entity"
# MySQL 用户名
MYSQL_USER = "spider"
# MySQL 密码
MYSQL_PASSWORD = "***"

# MySQL 地址(str)
MYSQL_HOST_LOCAL = "localhost"
# MySQL 端口(int)
MYSQL_PORT_LOCAL = 3306
# MySQL 数据库(str)
MYSQL_DATABASE_LOCAL = "huoguo"
# MySQL 表名(str)
MYSQL_TABLE_LOCAL = "huoguodian"
# MySQL 用户名

MYSQL_USER_LOCAL = "root"
# MySQL 密码

MYSQL_PASSWORD_LOCAL = "***"

# AI 模型路径
AI_PATH = "/root/INTERFACE/TEXTRANK.py"

CREATE_ID = "xxx"
CREATE_NAME = "xxx"

# 环境
# 生产环境
# ENV = "pro"
# 测试环境
ENV = "dev"
# ENV = "prod"

TABLE_NAME = lambda name: name if ENV == "pro" else "TEST_" + name
