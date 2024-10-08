import time
import datetime
import json
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import MariadbGtidEvent
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent
)

##################################################################################################
def check_binlog_settings(mysql_host=None, mysql_port=None, mysql_user=None,
                          mysql_passwd=None, mysql_charset=None):
    # 连接 MySQL 数据库
    source_mysql_settings = {
        "host": mysql_host,
        "port": mysql_port,
        "user": mysql_user,
        "passwd": mysql_passwd,
        "charset": mysql_charset
    }

    conn = pymysql.connect(**source_mysql_settings)
    cursor = conn.cursor()

    try:
        # 查询 binlog_format 的值
        cursor.execute("SHOW VARIABLES LIKE 'binlog_format'")
        row = cursor.fetchone()
        binlog_format = row[1]

        # 查询 binlog_row_image 的值
        cursor.execute("SHOW VARIABLES LIKE 'binlog_row_image'")
        row = cursor.fetchone()
        binlog_row_image = row[1]

        # 检查参数值是否满足条件
        if binlog_format != 'ROW' and binlog_row_image != 'FULL':
            exit("\nMySQL 的变量参数 binlog_format 的值应为 ROW，参数 binlog_row_image 的值应为 FULL\n")

    finally:
        # 关闭数据库连接
        cursor.close()
        conn.close()

##################################################################################################
def convert_bytes_to_str(data):
    if isinstance(data, dict):
        return {convert_bytes_to_str(key): convert_bytes_to_str(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_bytes_to_str(item) for item in data]
    elif isinstance(data, bytes):
        return data.decode('utf-8')
    else:
        return data
##################################################################################################


def process_binlogevent(binlogevent):
    database_name = binlogevent.schema
    sql_list = []

    for row in binlogevent.rows:
        if isinstance(binlogevent, WriteRowsEvent):
            values = convert_bytes_to_str(row["values"])
            sql = "REPLACE INTO {}({}) VALUES ({});".format(
                f"`{database_name}`.`{binlogevent.table}`" if database_name else binlogevent.table,
                ','.join(["`{}`".format(k) for k in values.keys()]),
                ','.join([
                    "'{}'".format(json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v)
                    if isinstance(v, (str, datetime.datetime, datetime.date, dict, list))
                    else 'NULL' if v is None
                    else str(v)
                    for v in values.values()
                ])
            )
            sql_list.append(sql)

        elif isinstance(binlogevent, UpdateRowsEvent):
            after_values = convert_bytes_to_str(row["after_values"])
            columns = ', '.join([f"`{k}`" for k in after_values.keys()])
            value_list = []
            for v in after_values.values():
                if isinstance(v, (dict, list)):
                    value_list.append(f"'{json.dumps(v, ensure_ascii=False)}'")
                elif isinstance(v, (str, datetime.datetime, datetime.date)):
                    value_list.append(f"'{v}'")
                elif v is None:
                    value_list.append("NULL")
                else:
                    value_list.append(str(v))
            values_str = ', '.join(value_list)
            sql = f"REPLACE INTO `{database_name}`.`{binlogevent.table}` ({columns}) VALUES ({values_str});"
            sql_list.append(sql)

        elif isinstance(binlogevent, DeleteRowsEvent):
            sql_list.append("delete")

    return sql_list
##################################################################################################
def parsing_binlog(mysql_host=None, mysql_port=None, mysql_user=None, mysql_passwd=None,
         mysql_database=None, mysql_charset=None, binlog_file=None, binlog_pos=None, slave_gtid=None):

    domain_id, server_id, gtid_number = slave_gtid[1].split("-")
    #print(domain_id, server_id, gtid_number)
    gtid_number_current = int(gtid_number) + 1
    gtid_number_next = int(gtid_number) + 2

    source_mysql_settings = {
        "host": mysql_host,
        "port": mysql_port,
        "user": mysql_user,
        "passwd": mysql_passwd,
        "database": mysql_database,
        "charset": mysql_charset
    }

    stream = BinLogStreamReader(
        connection_settings=source_mysql_settings,
        server_id=1234567890,
        blocking=False,
        resume_stream=True,
        only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, MariadbGtidEvent],
        log_file=binlog_file,
        log_pos=int(binlog_pos)
    )

    sql_r = []
    gtid_r = None  # 初始化 GTID 变量
    found_target = False

    for binlogevent in stream:
        if isinstance(binlogevent, MariadbGtidEvent):
            if binlogevent.gtid == f"{domain_id}-{server_id}-{gtid_number_current}":
                found_target = True
                gtid_r = binlogevent.gtid
            elif found_target and binlogevent.gtid == f"{domain_id}-{server_id}-{gtid_number_next}":
                break

        if found_target and isinstance(binlogevent, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)):
            result = process_binlogevent(binlogevent)
            sql_r.extend(result)

    stream.close()
    #print(f"sql_r: {sql_r}")
    #print(f"gtid_r: {gtid_r}")
    return sql_r, gtid_r

