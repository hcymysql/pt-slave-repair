import time, os, sys
import datetime
import json
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent
)
from pymysqlreplication.event import GtidEvent

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
            sys.exit("\nMySQL 的变量参数 binlog_format 的值应为 ROW，参数 binlog_row_image 的值应为 FULL\n")

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
         mysql_database=None, mysql_charset=None, binlog_file=None, binlog_pos=None, gtid_event=None):

    #print(f"gtid_event: {gtid_event}")
    gtid_server_uuid, gtid_number = gtid_event.split(":")
    gtid_number_next = int(gtid_number) + 1

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
        only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, GtidEvent],
        log_file=binlog_file,
        log_pos=int(binlog_pos)
    )

    sql_r = []
    found_target = False

    for binlogevent in stream:
        if isinstance(binlogevent, GtidEvent):
            if binlogevent.gtid == gtid_event:
                found_target = True
                #print(f"found_target: {found_target}")
            elif found_target and binlogevent.gtid == f"{gtid_server_uuid}:{gtid_number_next}":
                break

        if found_target and isinstance(binlogevent, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)):
            result = process_binlogevent(binlogevent)
            sql_r.extend(result)
 
    stream.close()
    #print(sql_r)
    # 添加空列表检查
    if not sql_r:  # 如果 sql_r 为空列表
        sys.exit("\033[33m\n未能成功解析主库的 binlog 日志信息，主程序已退出。\n\033[0m")
    return sql_r
