import sys
import pymysql
from pymysql.constants import CLIENT

class MySQL_Check(object):
    def __init__(self, host, port, user, password):
        self._host = host
        self._port = int(port)
        self._user = user
        self._password = password
        self._connection = None
        try:
            self._connection = pymysql.connect(host=self._host, port=self._port, user=self._user, passwd=self._password)
        except pymysql.Error as e:
            print("Error %d: %s" % (e.args[0], e.args[1]))
            sys.exit('error! MySQL can\'t connect!')


    def chek_repl_status(self):
        cursor = self._connection.cursor()

        try:
            if cursor.execute('SHOW SLAVE HOSTS') >= 1 and cursor.execute('SHOW SLAVE STATUS') == 0:
                print(f"{self._host}:{self._port} 这是一台主库，环境不匹配！")
                sys.exit(2)
            elif cursor.execute('SHOW SLAVE HOSTS') == 0 and cursor.execute('SHOW SLAVE STATUS') == 1:
                #print(f"cursor.execute('SHOW SLAVE HOSTS'):  {cursor.execute('SHOW SLAVE HOSTS')}")
                #print("这是一台从库")
                pass
            elif cursor.execute('SHOW SLAVE HOSTS') >= 1 and cursor.execute('SHOW SLAVE STATUS') == 1:
                pass
            else:
                print(f"{self._host}:{self._port} 这台机器你没有设置主从复制，环境不匹配！")
                sys.exit(2)
        except pymysql.Error as e:
            print("Error %d: %s" % (e.args[0], e.args[1]))
            sys.exit('MySQL Replication Health is NOT OK!')
        finally:
            cursor.close()


    def get_slave_status(self):
        cursor = self._connection.cursor(cursor=pymysql.cursors.DictCursor)  # 以字典的形式返回操作结果

        try:
            cursor.execute('SHOW SLAVE STATUS')
            slave_status_dict = cursor.fetchone()
        except pymysql.Error as e:
            print("Error %d: %s" % (e.args[0], e.args[1]))
        finally:
            cursor.close()

        return slave_status_dict


    def get_gtid_status(self):
        cursor = self._connection.cursor()

        try:
            cursor.execute('SHOW GLOBAL VARIABLES WHERE variable_name = \'gtid_mode\'')
            gtid_result = cursor.fetchone()
        except pymysql.Error as e:
            print("Error %d: %s" % (e.args[0], e.args[1]))
        finally:
            cursor.close()

        return gtid_result


    def get_para_workers(self):
        cursor = self._connection.cursor()

        try:
            cursor.execute('SHOW GLOBAL VARIABLES WHERE variable_name = \'slave_parallel_workers\'')
            s_workers_result = cursor.fetchone()
        except pymysql.Error as e:
            print("Error %d: %s" % (e.args[0], e.args[1]))
        finally:
            cursor.close()

        return s_workers_result


    def turn_off_parallel(self):
        self._connection = pymysql.connect(host=self._host, port=self._port, user=self._user, passwd=self._password, client_flag=CLIENT.MULTI_STATEMENTS)
        cursor = self._connection.cursor()

        try:
            cursor.execute('STOP SLAVE SQL_THREAD; SET GLOBAL slave_parallel_workers = 0; START SLAVE SQL_THREAD')
        except pymysql.Error as e:
            print("Error %d: %s" % (e.args[0], e.args[1]))
            return False
        finally:
            cursor.close()

        return True


    def turn_on_parallel(self, slave_parallel_workers):
        self._connection = pymysql.connect(host=self._host, port=self._port, user=self._user, passwd=self._password, client_flag=CLIENT.MULTI_STATEMENTS)
        cursor = self._connection.cursor()

        try:
            cursor.execute(f'STOP SLAVE SQL_THREAD; SET GLOBAL slave_parallel_workers = {slave_parallel_workers}; START SLAVE SQL_THREAD')
        except pymysql.Error as e:
            print("Error %d: %s" % (e.args[0], e.args[1]))
            return False
        finally:
            cursor.close()

        return True


    def get_slave_error(self):
        cursor = self._connection.cursor(cursor=pymysql.cursors.DictCursor)  # 以字典的形式返回操作结果

        try:
            cursor.execute('select LAST_ERROR_NUMBER,LAST_ERROR_MESSAGE,LAST_ERROR_TIMESTAMP '
                           'from performance_schema.replication_applier_status_by_worker '
                           'ORDER BY LAST_ERROR_TIMESTAMP desc limit 1')
            error_dict = cursor.fetchone()
        except pymysql.Error as e:
            print("Error %d: %s" % (e.args[0], e.args[1]))
        finally:
            cursor.close()

        return error_dict


    def fix_error_enable_binlog(self, repair_sql):
        cursor = self._connection.cursor()
        affected_rows = 0

        try:
            # 开始事务
            self._connection.begin()

            cursor.execute(repair_sql)
            affected_rows = cursor.rowcount

            # 提交事务
            self._connection.commit()
        except pymysql.Error as e:
            # 回滚事务
            self._connection.rollback()
            print("Error %d: %s" % (e.args[0], e.args[1]))
        finally:
            cursor.close()

        return affected_rows


    def fix_error_disable_binlog(self, repair_sql):
        cursor = self._connection.cursor()
        affected_rows = 0

        try:
            cursor.execute("SET SESSION SQL_LOG_BIN = OFF")  # 在事务外设置 sql_log_bin 的值
            # 开始事务
            self._connection.begin()
            cursor.execute(repair_sql)
            affected_rows = cursor.rowcount

            # 提交事务
            self._connection.commit()
        except pymysql.Error as e:
            # 回滚事务
            self._connection.rollback()
            print("Error %d: %s" % (e.args[0], e.args[1]))
        finally:
            cursor.close()

        return affected_rows


    def unset_super_read_only(self):
        self._connection = pymysql.connect(host=self._host, port=self._port, user=self._user, passwd=self._password)
        cursor = self._connection.cursor()
        try:
            cursor.execute('SET GLOBAL SUPER_READ_ONLY = 0')
            cursor.execute('SET GLOBAL READ_ONLY = 0')
        except pymysql.Error as e:
            print("Error %d: %s" % (e.args[0], e.args[1]))
            return False
        finally:
            cursor.close()
        return True


    def set_super_read_only(self):
        self._connection = pymysql.connect(host=self._host, port=self._port, user=self._user, passwd=self._password)
        cursor = self._connection.cursor()
        try:
            cursor.execute('SET GLOBAL SUPER_READ_ONLY = 1')
        except pymysql.Error as e:
            print("Error %d: %s" % (e.args[0], e.args[1]))
            return False
        finally:
            cursor.close()
        return True


    def skip_gtid(self, gtid_value):
        self._connection = pymysql.connect(host=self._host, port=self._port, user=self._user, passwd=self._password, client_flag=CLIENT.MULTI_STATEMENTS)
        cursor = self._connection.cursor()
        try:
            skip_gtid_sql = 'STOP SLAVE; SET gtid_next = \'{0}\'; BEGIN;COMMIT; SET gtid_next = \'AUTOMATIC\' ' \
                         .format(gtid_value)
            cursor.execute(skip_gtid_sql)
        except pymysql.Error as e:
            print("Error %d: %s" % (e.args[0], e.args[1]))
            return False
        finally:
            cursor.close()

        return True


    def skip_position(self):
        self._connection = pymysql.connect(host=self._host, port=self._port, user=self._user, passwd=self._password, client_flag=CLIENT.MULTI_STATEMENTS)
        cursor = self._connection.cursor()
        try:
            skip_pos_sql = 'STOP SLAVE; SET GLOBAL SQL_SLAVE_SKIP_COUNTER=1'
            cursor.execute(skip_pos_sql)
        except pymysql.Error as e:
            print("Error %d: %s" % (e.args[0], e.args[1]))
            return False
        finally:
            cursor.close()

        return True


    def start_slave(self):
        self._connection = pymysql.connect(host=self._host, port=self._port, user=self._user, passwd=self._password)
        cursor = self._connection.cursor()
        try:
            start_slave_sql = 'START SLAVE'
            cursor.execute(start_slave_sql)
        except pymysql.Error as e:
            print("Error %d: %s" % (e.args[0], e.args[1]))
            return False
        finally:
            cursor.close()

        return True
