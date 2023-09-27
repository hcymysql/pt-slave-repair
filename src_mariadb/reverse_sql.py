import time, os, sys
import datetime
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
def process_binlogevent(binlogevent):
    database_name = binlogevent.schema
    sql_list = []

    for row in binlogevent.rows:
        if isinstance(binlogevent, WriteRowsEvent):
            sql = "REPLACE INTO {}({}) VALUES ({});".format(
                f"`{database_name}`.`{binlogevent.table}`" if database_name else binlogevent.table,
                ','.join(["`{}`".format(k) for k in row["values"].keys()]),
                ','.join(["'{}'".format(v) if isinstance(v, (
                    str, datetime.datetime, datetime.date)) else 'NULL' if v is None else str(v)
                          for v in row["values"].values()])
            )
            sql_list.append(sql)

        elif isinstance(binlogevent, UpdateRowsEvent):
            rollback_replace_set_values = []
            for v in row["after_values"].values():
                if v is None:
                    rollback_replace_set_values.append("NULL")
                elif isinstance(v, (str, datetime.datetime, datetime.date)):
                    rollback_replace_set_values.append(f"'{v}'")
                else:
                    rollback_replace_set_values.append(str(v))
            rollback_replace_set_clause = ','.join(rollback_replace_set_values)
            fields_clause = ','.join([f"`{k}`" for k in row["before_values"].keys()])
            sql = f"REPLACE INTO `{database_name}`.`{binlogevent.table}` ({fields_clause}) VALUES ({rollback_replace_set_clause});"
            sql_list.append(sql)

        elif isinstance(binlogevent, DeleteRowsEvent):
            sql_list.append("delete")

    return sql_list
##################################################################################################
def parsing_binlog(mysql_host=None, mysql_port=None, mysql_user=None, mysql_passwd=None,
         mysql_database=None, mysql_charset=None, binlog_file=None, binlog_pos=None):

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
        only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
        log_file=binlog_file,
        log_pos=int(binlog_pos)
    )

    sql_r = []
    last_event = None

    for binlogevent in stream:
        # 检查每次读取到新的事件时，检查当前事件是否与上一个事件属于同一个事务。
        # 如果是，则继续处理，如果不是，则退出循环。
        if last_event and binlogevent.packet.log_pos != last_event.packet.log_pos:
            break
        last_event = binlogevent

        result = process_binlogevent(binlogevent)
        sql_r.extend(result)
    stream.close()
    #print(sql_r)
    return sql_r

