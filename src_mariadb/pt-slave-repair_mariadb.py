import re, time, os, sys
import pymysql
from check_repl_mariadb import MySQL_Check
from reverse_sql import *
import argparse
import signal
import logging

# 创建ArgumentParser对象
parser = argparse.ArgumentParser(description='自动修复MySQL主从同步报错数据')

# 添加命令行参数
parser.add_argument('-H', '--slave_ip', type=str, help='Slave IP', required=True)
parser.add_argument('-P', '--slave_port', type=int, help='Slave Port', required=True)
parser.add_argument('-u', '--slave_user', type=str, help='Slave User', required=True)
parser.add_argument('-p', '--slave_password', type=str, help='Slave Password', required=True)
parser.add_argument('-d', '--db_name', type=str, help='Database Name', required=True)
parser.add_argument('-e', '--enable-binlog', dest='enable_binlog', action='store_true', default=False, help='Enable binary logging of the restore data')

# 解析命令行参数
args = parser.parse_args()

# 获取变量值
slave_ip = args.slave_ip
slave_port = args.slave_port
slave_user = args.slave_user
slave_password = args.slave_password
enable_binlog = args.enable_binlog
db_name = args.db_name

# 获取当前脚本所在目录（包括打包后的情况）
if getattr(sys, 'frozen', False):
    # 打包后的情况
    current_dir = os.path.dirname(sys.executable)
else:
    # 未打包的情况
    current_dir = os.path.dirname(os.path.abspath(__file__))

# 创建log目录（如果不存在）
log_dir = os.path.join(current_dir, "log")
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# 设置日志文件路径为log目录下的文件
log_file_path = os.path.join(log_dir, f"{db_name}_INFO.log")

# 创建日志处理器
logger = logging.getLogger()
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(logging.INFO)
log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)


def signal_handler(sig, frame):
    logger.info('程序被终止')
    sys.exit(0)

# 注册信号处理函数
signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
signal.signal(signal.SIGTSTP, signal_handler)  # Ctrl+Z

mysql_conn = MySQL_Check(host=slave_ip, port=slave_port, user=slave_user, password=slave_password)

ok_count = 0
while True:
    mysql_conn.chek_repl_status()

    # 检测show slave status同步状态
    r_dict = mysql_conn.get_slave_status()

    # 获取slave_parallel_workers线程数量
    slave_workers = mysql_conn.get_para_workers()
    slave_workers = int(slave_workers[1])

    if r_dict['Slave_IO_Running'] == 'Yes' and r_dict['Slave_SQL_Running'] == 'Yes':
        ok_count += 1
        if ok_count < 2:
            logger.info('\033[1;36m同步正常. \033[0m')

    elif (r_dict['Slave_IO_Running'] == 'Yes' and r_dict['Slave_SQL_Running'] == 'No') \
            or (r_dict['Slave_IO_Running'] == 'No' and r_dict['Slave_SQL_Running'] == 'No'):
        logger.error('\033[1;31m主从复制报错. Slave_IO_Running状态值是：%s '
                      ' |  Slave_SQL_Running状态值是：%s  \n  \tLast_Error错误信息是：%s'
                      '  \n\n  \tLast_SQL_Error错误信息是：%s \033[0m' \
                      % (r_dict['Slave_IO_Running'], r_dict['Slave_SQL_Running'], \
                         r_dict['Last_Error'], r_dict['Last_SQL_Error']))
        error_dict = mysql_conn.get_slave_error()
        if error_dict is not None:
            logger.error('错误号是：%s' % error_dict.get('LAST_ERROR_NUMBER'))
            logger.error('错误信息是：%s' % error_dict.get('LAST_ERROR_MESSAGE'))
            logger.error('报错时间是：%s\n' % error_dict.get('LAST_ERROR_TIMESTAMP'))
        logger.info('-' * 100)
        logger.info('开始自动修复同步错误的数据......\n')

        # binlog环境检查
        check_binlog_settings(mysql_host=slave_ip, mysql_port=slave_port, mysql_user=slave_user,
                              mysql_passwd=slave_password, mysql_charset="utf8")

        # 获取slave info信息
        master_host = r_dict['Master_Host']
        master_user = r_dict['Master_User']
        master_port = int(r_dict['Master_Port'])
        relay_master_log_file = r_dict['Relay_Master_Log_File']
        exec_master_log_pos = r_dict['Exec_Master_Log_Pos']
        last_sql_errno = int(r_dict['Last_SQL_Errno'])
        Using_Gtid = r_dict['Using_Gtid']

        # 获取修复数据的SQL语句
        if last_sql_errno in (1062, 1032):
            repair_sql_list, gtid_number = parsing_binlog(mysql_host=master_host, mysql_port=master_port, mysql_user=master_user, mysql_passwd=slave_password,
                                    mysql_charset='utf8', binlog_file=relay_master_log_file, binlog_pos=exec_master_log_pos)
            #print(f"SQL语句：{repair_sql_list}") 调试
            #print(f"GTID事务号：{gtid_number}")
            for count, repair_sql in enumerate(repair_sql_list, 1):
                logger.info(f"修复数据的SQL语句: {repair_sql}")

                # 判断修复数据的SQL是否有DELETE
                pattern = re.compile(r'^delete', re.IGNORECASE)
                if pattern.match(repair_sql): #如果匹配上了DELETE，直接跳过错误，不做处理。
                    # 判断从库是否开启了基于GTID的复制
                    if Using_Gtid == "NO": #基于Position位置点复制
                        mysql_conn.turn_off_parallel()
                        time.sleep(0.3)
                        skip_pos_r = mysql_conn.skip_position()
                        if skip_pos_r:
                            logger.info("成功修复了 【%d】 行数据" % count)
                            # 再开启多线程并行复制
                            mysql_conn.turn_on_parallel(slave_workers)
                    else: #基于GTID事务号复制
                        mysql_conn.turn_off_parallel()
                        time.sleep(0.3)
                        skip_pos_r = mysql_conn.skip_gtid(gtid_number)
                        if skip_pos_r:
                            logger.info("成功修复了 【%d】 行数据" % count)
                            # 再开启多线程并行复制
                            mysql_conn.turn_on_parallel(slave_workers)
                else: #如果匹配上了UPDATE/INSERT，修复错误数据。
                    # 先关闭只读
                    mysql_conn.unset_super_read_only()
                    if enable_binlog:
                        fix_result = mysql_conn.fix_error_enable_binlog(repair_sql)
                    else:
                        fix_result = mysql_conn.fix_error_disable_binlog(repair_sql)
                    if fix_result > 0:
                        # 判断从库是否开启了基于GTID的复制
                        if Using_Gtid == "NO":  # 基于Position位置点复制
                            mysql_conn.turn_off_parallel()
                            time.sleep(0.3)
                            skip_pos_r = mysql_conn.skip_position()
                            if skip_pos_r:
                                logger.info("成功修复了 【%d】 行数据" % count)
                                # 再开启多线程并行复制
                                mysql_conn.turn_on_parallel(slave_workers)
                        else:
                            mysql_conn.turn_off_parallel()
                            time.sleep(0.3)
                            skip_pos_r = mysql_conn.skip_gtid(gtid_number)
                            if skip_pos_r:
                                logger.info("成功修复了 【%d】 行数据" % count)
                                # 再开启多线程并行复制
                                mysql_conn.turn_on_parallel(slave_workers)
                        # 开启只读
                        mysql_conn.set_super_read_only()
                    else:
                        logger.info("未更改数据，请查看error.log文件以获取错误信息，并进行问题诊断。")
                        # 开启只读
                        mysql_conn.set_super_read_only()
                        break
        else:
            logger.info('只处理错误号1032和1062同步报错的数据修复。')
            break

    time.sleep(1)
# END while True
##################################################################################################
