import re, time, os, sys
import pymysql
from check_repl_mysql import MySQL_Check
from reverse_sql import *
import argparse
import signal
import logging

# 创建ArgumentParser对象
parser = argparse.ArgumentParser(description=
"""
自动修复MySQL主从同步报错数据 \n
 - The automatic repair of data synchronization errors(1032/1062) between MySQL master and slave. 
""", formatter_class=argparse.RawTextHelpFormatter)

# 添加命令行参数
parser.add_argument('-H', '--slave_ip', type=str, help='Slave IP', required=True)
parser.add_argument('-P', '--slave_port', type=int, help='Slave Port', required=True)
parser.add_argument('-u', '--slave_user', type=str, help='Slave Repl User', required=True)
parser.add_argument('-p', '--slave_password', type=str, help='Slave Repl Password', required=True)
parser.add_argument('-d', '--db_name', type=str, help='Your Database Name', required=True)
parser.add_argument('-e', '--enable-binlog', dest='enable_binlog', action='store_true', default=False, help='Enable binary logging of the restore data')
parser.add_argument('-v', '--version', action='version', version='pt-slave-repair工具版本号: 1.0.9，更新日期：2025-03-25')

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

    # 获取GTID状态
    r_gtid = mysql_conn.get_gtid_status()
    r_gtid = r_gtid[1].upper()

    # 获取slave_parallel_workers线程数量
    slave_workers = mysql_conn.get_para_workers()
    slave_workers = int(slave_workers[1])

    if r_dict['Slave_IO_Running'] == 'Yes' and r_dict['Slave_SQL_Running'] == 'Yes':
        ok_count += 1
        if ok_count < 2:
            if r_gtid == "ON" and r_dict['Auto_Position'] != 1:
                logger.warning('\033[1;33m开启基于GTID全局事务ID复制，CHANGE MASTER TO MASTER_AUTO_POSITION = 1 需要设置为1. \033[0m')
            logger.info('\033[1;36m同步正常. \033[0m')

    elif (r_dict['Slave_IO_Running'] == 'Yes' and r_dict['Slave_SQL_Running'] == 'No') \
            or (r_dict['Slave_IO_Running'] == 'No' and r_dict['Slave_SQL_Running'] == 'No'):
        logger.error('\033[1;31m主从复制报错. Slave_IO_Running状态值是：%s '
                      ' |  Slave_SQL_Running状态值是：%s  \n  \tLast_Error错误信息是：%s'
                      '  \n\n  \tLast_SQL_Error错误信息是：%s \033[0m' \
                      % (r_dict['Slave_IO_Running'], r_dict['Slave_SQL_Running'], \
                         r_dict['Last_Error'], r_dict['Last_SQL_Error']))
        error_dict = mysql_conn.get_slave_error()
        if error_dict is not None: # 判断performance_schema参数是否开启
            logger.error('错误号是：%s' % error_dict['LAST_ERROR_NUMBER'])
            logger.error('错误信息是：%s' % error_dict['LAST_ERROR_MESSAGE'])
            logger.error('报错时间是：%s\n' % error_dict['LAST_ERROR_TIMESTAMP'])
        logger.info('-' * 100)
        logger.info('开始自动修复同步错误的数据......\n')

        # binlog环境检查
        check_binlog_settings(mysql_host=slave_ip, mysql_port=slave_port, mysql_user=slave_user,
                              mysql_passwd=slave_password, mysql_charset="utf8mb4")

        # 获取slave info信息
        master_host = r_dict['Master_Host']
        master_user = slave_user
        master_port = int(r_dict['Master_Port'])
        relay_master_log_file = r_dict['Relay_Master_Log_File']
        exec_master_log_pos = r_dict['Exec_Master_Log_Pos']
        retrieved_gtid_set = r_dict['Retrieved_Gtid_Set']
        executed_gtid_set = r_dict['Executed_Gtid_Set']
        last_sql_errno = int(r_dict['Last_SQL_Errno'])
        #print(f"retrieved_gtid_set: {retrieved_gtid_set}")
        #print(f"executed_gtid_set: {executed_gtid_set}")

        executed_gtid_list = []
        # 提取每个 GTID 的集合
        retrieved_gtid_list = re.findall(r'(\w+-\w+-\w+-\w+-\w+:\d+-\d+|\w+-\w+-\w+-\w+-\w+:\d+)', retrieved_gtid_set)
        if executed_gtid_set == "" or executed_gtid_set is None:
            executed_gtid_list = [retrieved_gtid_set]
        else:
            executed_gtid_list = re.findall(r'(\w+-\w+-\w+-\w+-\w+:\d+-\d+|\w+-\w+-\w+-\w+-\w+:\d+)', executed_gtid_set)
        #print(f"retrieved_gtid_list: {retrieved_gtid_list}")
        #print(f"executed_gtid_list: {executed_gtid_list}") #调试

        gtid_domain = None
        gtid_range_value = None
        gtid_range = None
        gtid_number = 0

        # 检查 Executed_Gtid_Set 是否在 Retrieved_Gtid_Set 中
        for gtid in executed_gtid_list:
            if any(gtid.split(':')[0] in retrieved for retrieved in retrieved_gtid_list):
                gtid_parts = gtid.split(':')
                gtid_domain = gtid_parts[0]
                gtid_range = gtid_parts[1]

                if '-' in gtid_range:
                    gtid_range_parts = gtid_range.split('-')
                    gtid_range_value = gtid_range_parts[-1]

        # 获取修复数据的SQL语句
        if last_sql_errno in (1062, 1032):
            if gtid_range is not None and '-' not in str(gtid_range):
                gtid_number = int(gtid_range) + 1
            if gtid_range_value is not None:
                gtid_number = int(gtid_range_value) + 1
            gtid_TXID = f"{gtid_domain}:{gtid_number}"

            try:
                repair_sql_list = parsing_binlog(mysql_host=master_host, mysql_port=master_port, mysql_user=master_user, mysql_passwd=slave_password,
                                    mysql_charset='utf8mb4', binlog_file=relay_master_log_file, binlog_pos=exec_master_log_pos, gtid_event=gtid_TXID)
                if repair_sql_list is None:
                    logger.error("没有捕获到正确的GTID事件，请检查change master to的时候master_auto_position是等于1吗？show slave status看看Auto_Position的值是不是为1。")
                    sys.exit(1)
            except Exception as e:
                # 在捕获到异常时使用 sys.exit() 终止程序
                logger.error(f"An error occurred: {str(e)}")
                sys.exit(1)
            for count, repair_sql in enumerate(repair_sql_list, 1):
                logger.info(f"修复数据的SQL语句: {repair_sql}")

                # 判断修复数据的SQL是否有DELETE
                pattern = re.compile(r'^delete', re.IGNORECASE)
                if pattern.match(repair_sql): #如果匹配上了DELETE，直接跳过错误，不做处理。
                    # 判断从库是否开启了基于GTID的复制
                    if r_gtid != "ON": #基于Position位置点复制
                        mysql_conn.turn_off_parallel()
                        time.sleep(0.3)
                        skip_pos_r = mysql_conn.skip_position()
                        if skip_pos_r:
                            logger.info("成功修复了 【%d】 行数据" % count)

                    else: #基于GTID事务号复制
                        if gtid_range is not None and '-' not in str(gtid_range):
                            gtid_number = int(gtid_range) + 1
                        if gtid_range_value is not None:
                            gtid_number = int(gtid_range_value) + 1
                        gtid_TXID = f"{gtid_domain}:{gtid_number}"

                        """
                        参考pt-slave-restart实现原理，要关闭多线程并行复制，然后再跳过出错的GTID事件号。
                        pt-slave-restart will not skip transactions when multiple replication threads are being used (slave_parallel_workers > 0). 
                        pt-slave-restart does not know what the GTID event is of the failed transaction of a specific slave thread. 
                        """
                        mysql_conn.turn_off_parallel()
                        time.sleep(0.3)

                        # 跳过出错的GTID事件号
                        skip_gtid_r = mysql_conn.skip_gtid(gtid_TXID)
                        if skip_gtid_r:
                            count += 1
                            logger.info("成功修复了 【%d】 行数据" % count)

                else: #如果匹配上了UPDATE/INSERT，修复错误数据。
                    # 先关闭只读
                    mysql_conn.unset_super_read_only()
                    if enable_binlog:
                        try:
                            fix_result = mysql_conn.fix_error_enable_binlog(repair_sql)
                        except Exception as e:
                            # 在捕获到异常时使用 sys.exit() 终止程序
                            logger.error(f"An error occurred: {str(e)}")
                            sys.exit(1)
                    else:
                        try:
                            fix_result = mysql_conn.fix_error_disable_binlog(repair_sql)
                        except Exception as e:
                            # 在捕获到异常时使用 sys.exit() 终止程序
                            logger.error(f"An error occurred: {str(e)}")
                            sys.exit(1)
                    if fix_result > 0:
                        # 判断从库是否开启了基于GTID的复制
                        if r_gtid != "ON":  # 基于Position位置点复制
                            mysql_conn.turn_off_parallel()
                            time.sleep(0.3)
                            skip_pos_r = mysql_conn.skip_position()
                            if skip_pos_r:
                                logger.info("成功修复了 【%d】 行数据" % count)
                        else:  # 基于GTID事务号复制
                            if gtid_range is not None and '-' not in str(gtid_range):
                                gtid_number = int(gtid_range) + 1
                            if gtid_range_value is not None:
                                gtid_number = int(gtid_range_value) + 1
                            gtid_TXID = f"{gtid_domain}:{gtid_number}"

                            """
                            参考pt-slave-restart实现原理，要关闭多线程并行复制，然后再跳过出错的GTID事件号。
                            pt-slave-restart will not skip transactions when multiple replication threads are being used (slave_parallel_workers > 0). 
                            pt-slave-restart does not know what the GTID event is of the failed transaction of a specific slave thread. 
                            """
                            mysql_conn.turn_off_parallel()
                            time.sleep(0.3)

                            skip_gtid_r = mysql_conn.skip_gtid(gtid_TXID)
                            if skip_gtid_r:
                                logger.info("成功修复了 【%d】 行数据" % count)
                        # 开启只读
                        mysql_conn.set_super_read_only()
                    else:
                        logger.error(f"未更改数据，请查看{db_name}_INFO.log文件以获取错误信息，并进行问题诊断。")
                        # 开启只读
                        mysql_conn.set_super_read_only()
                        break

            # 修复数据后，开启START SLAVE
            mysql_conn.start_slave()
            # 再开启多线程并行复制
            mysql_conn.turn_on_parallel(slave_workers)

        else:
            logger.info('只处理错误号1032和1062同步报错的数据修复。')
            break

    time.sleep(1)
# END while True
##################################################################################################
