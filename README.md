# pt-slave-repair介绍
https://www.oschina.net/p/pt-slave-repair

MySQL主从复制作为一种常见的数据同步方式，有时候会出现同步错误导致同步中断的情况。手动修复这些同步错误通常需要耗费时间和精力，并且对于不熟悉MySQL复制的人来说比较困难。

#### pt-slave-repair是对原有pt-slave-restart工具的补充，它提供自动修复MySQL主从同步复制的报错数据，以及恢复中断的sql thread复制线程。

```
pt-slave-repair 工具版本号: 1.0.8，更新日期：2024-08-13 - 支持解析json

二进制文件下载地址：
https://github.com/hcymysql/pt-slave-repair/releases/tag/pt-slave-repair_v1.0.8
```

![image](https://github.com/hcymysql/pt-slave-repair/assets/19261879/d71bcceb-d7ba-4aff-b631-32d914810e6e)

-----------------------------------------------------------------------
自动修复主从同步数据工具具有以下几个优势：

    1）提高效率：自动修复工具能够快速检测和修复主从同步错误，无需手动干预。这大大节省了DBA的时间和精力，使他们能够更专注于其他重要的任务。

    2）减少人为错误：手动修复同步错误可能存在人为错误的风险，例如配置错误或操作失误。自动修复工具可以提供一致性和准确性的修复策略，减少了人为错误的可能性。

    3）实时监控和响应：具有实时监控功能，可以及时检测同步错误的发生，并立即采取相应的修复措施。这有助于及时恢复同步并减少数据延迟。

    4）自动化运维：可以定期检查主从同步状态并执行修复操作，无需人工干预。这减少了对人工操作和监控的依赖，提高了系统的可靠性和稳定性。

    5）快速故障恢复：当主从同步错误发生时，自动修复工具能够迅速识别和修复问题，从而减少数据丢失和业务中断的时间。这有助于提高系统的可用性和数据的一致性。

总的来说，自动修复主从同步数据工具能够提高效率、降低风险、实时监控和响应、自动化运维以及快速故障恢复，可以极大地提升同步运行的稳定性和可靠性。

# 原理
```
1） 当检测到同步报错1062（主键冲突、重复）和1032（数据丢失）时，首先要进行binlog环境检查，如果binlog_format不等于ROW并且binlog_row_image不等于FULL，则退出主程序。
    如果错误号非1062或1032，则直接退出主程序。

2） 获取show slave status信息，得到binlog、position、gtid信息

3） 连接到主库上解析binlog，如果是DELETE删除语句，则直接跳过

4)  关闭slave_parallel_workers多线程并行复制

5)  如果开启GITD复制模式，启用SET gtid_next方式；如果开启位置点复制模式，启动SET GLOBAL SQL_SLAVE_SKIP_COUNTER=1方式）

6） 如果是UPDATE/INSERT语句，则把BINLOG解析为具体的SQL，并且反转SQL，将其转换为REPLACE INTO

7） 将解析后的REPLACE INTO语句反向插入slave上，使其数据保持一致，然后执行第5步操作；

8） 将slave设置为read_only只读模式

9） 依次类推，最终使其show slave status同步为双YES（同步正常）。
```

![image](https://github.com/hcymysql/pt-slave-repair/assets/19261879/f263eded-85ef-4629-b497-9927bfaa70db)


# 工具演示视频：https://edu.51cto.com/video/1658.html

# 使用
```
shell> chmod 755 pt-slave-repair
```

# 连接到同步报错的slave从库上执行（请用MySQL复制的账号，例如repl，并赋予工具运行的权限）
# repl账号最小化权限：
```
mysql> show grants for repl@'%';
+------------------------------------------------------------------------+
| Grants for repl@%                                                       |
+------------------------------------------------------------------------+
| GRANT SUPER, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `repl`@`%`|
| GRANT SELECT, INSERT, UPDATE, DELETE ON `yourDB`.* TO `repl`@`%`         |
| GRANT SELECT ON `performance_schema`.* TO `repl`@`%`                   |
+------------------------------------------------------------------------+
3 rows in set (0.00 sec)
```

### 一、前台运行
```
shell> ./pt-slave-repair -H 192.168.198.239 -P 3346 -u repl -p hechunyang -d test
```
#### 注：你可以按<ctrl+c>或者<ctrl+z>退出程序。

### 二、后台运行
```
shell> nohup ./pt-slave-repair -H 192.168.198.239 -P 3346 -u repl -p hechunyang -d test > /dev/null &
```
#### 注：你可以
```
shell> pkill pt-slave-repair
```
退出后台进程。

### -e, --enable-binlog   Enable binary logging of the restore data

#### 1) -e 选项，默认修复完的数据不会记录在binlog文件里，如果你的slave是二级从库（后面还接着一个slave），那么开启这个选项。

#### 2) 开启后台守护进程后，会自动在当前目录下创建一个log目录和{db_name}_INFO.log文件，该文件保存着日志信息。

![image](https://github.com/hcymysql/pt-slave-repair/assets/19261879/743a5585-d78e-41d9-8e89-7dafe6d93222)

![image](https://github.com/hcymysql/pt-slave-repair/assets/19261879/a92170ef-cd65-467b-b055-b852732a3076)

### 支持 MySQL5.7/8.0 和 MariaDB 数据库，工具适用于Centos7 系统。

# 测试：

1） 先把主从复制环境配置好，然后在主库上插入3条数据，此时从库已经同步完该3条数据，然后在从库truncate掉该表，再到主库上update全表更新，这样就模拟出了1032错误。

2） 运行pt-slave-repair工具修复。
