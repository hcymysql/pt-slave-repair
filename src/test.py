# 测试用例
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import GtidEvent
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent
)

MYSQL_SETTINGS = {
    "host": "192.168.198.239",
    "port": 6666,
    "user": "admin",
    "passwd": "hechunyang"
}

def main():
    stream = BinLogStreamReader(connection_settings=MYSQL_SETTINGS,
                                server_id=123456789,
                                only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, GtidEvent],
                                log_file='mysql-bin.000001',
                                log_pos=4,
                                blocking=True)

    for binlogevent in stream:
        if isinstance(binlogevent, GtidEvent):
            print(f"GTID: {binlogevent.gtid}")
        elif isinstance(binlogevent, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)):
            print(f"GTID: {binlogevent.schema}")
            #print(binlogevent.rows)
        
        #binlogevent.dump()
    
    stream.close()

if __name__ == "__main__":
    main()

