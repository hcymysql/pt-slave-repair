# pt-slave-repair介绍

MySQL主从复制作为一种常见的数据同步方式，有时候会出现同步错误导致同步中断的情况。手动修复这些同步错误通常需要耗费时间和精力，并且对于不熟悉MySQL复制的人来说比较困难。

#### pt-slave-repair是对原有pt-slave-restart工具的补充，它提供自动修复MySQL主从同步复制的报错数据，以及恢复中断的sql thread复制线程。

自动修复主从同步数据工具具有以下几个优势：

    1）提高效率：自动修复工具能够快速检测和修复主从同步错误，无需手动干预。这大大节省了DBA的时间和精力，使他们能够更专注于其他重要的任务。

    2）减少人为错误：手动修复同步错误可能存在人为错误的风险，例如配置错误或操作失误。自动修复工具可以提供一致性和准确性的修复策略，减少了人为错误的可能性。

    3）实时监控和响应：自动修复工具通常具有实时监控功能，可以及时检测同步错误的发生，并立即采取相应的修复措施。这有助于及时恢复同步并减少数据延迟。

    4）自动化运维：自动修复工具可以定期检查主从同步状态并执行修复操作，无需人工干预。这减少了对人工操作和监控的依赖，提高了系统的可靠性和稳定性。

    5）快速故障恢复：当主从同步错误发生时，自动修复工具能够迅速识别和修复问题，从而减少数据丢失和业务中断的时间。这有助于提高系统的可用性和数据的一致性。

总的来说，自动修复主从同步数据工具能够提高效率、降低风险、实时监控和响应、自动化运维以及快速故障恢复，可以极大地提升同步运行的稳定性和可靠性。

# 使用
```
shell> cd pt-slave-repair/
shell> pip3 install -r requirements.txt
```

# pt-slave-repair会针对1062和1032的错误进行数据修复

### 一、前台运行
```
shell> python3 pt-slave-repair.py -H 192.168.198.239 -P 3346 -u admin -p hechunyang -d test
```
#### 注：你可以按<ctrl+c>或者<ctrl+z>退出程序。

### 二、后台运行
```
shell> python3 pt-slave-repair.py -H 192.168.198.239 -P 3346 -u admin -p hechunyang -d test --daemon
```
#### 注：你可以
```
shell> kill `cat /tmp/test_daemon.pid`
```
退出程序。

### -e, --enable-binlog   Enable binary logging of the restore data

#### 1) -e 选项，默认修复完的数据不会记录在binlog文件里，如果你的slave是二级从库（后面还接着一个slave），那么开启这个选项。

#### 2) 开启后台守护进程后，会自动在当前目录下创建一个log目录和{db_name}_INFO.log文件，该文件保存着日志信息。

#### 3) 开启后台守护进程后，pid文件在/tmp目录下，/tmp/{db_name}_daemon.pid

![image](https://github.com/hcymysql/pt-slave-repair/assets/19261879/a92170ef-cd65-467b-b055-b852732a3076)

