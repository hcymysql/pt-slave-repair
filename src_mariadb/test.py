# ≤‚ ‘”√¿˝
from pymysqlreplication import BinLogStreamReader
#from pymysqlreplication.event import GtidEvent
from pymysqlreplication.event import MariadbGtidEvent
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent
)

MYSQL_SETTINGS = {
    "host": "192.168.198.239",
    "port": 4307,
    "user": "admin",
    "passwd": "hechunyang"
}

def main():
    stream = BinLogStreamReader(connection_settings=MYSQL_SETTINGS,
                                server_id=3,
                                #only_events=[GtidEvent],
                                only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, MariadbGtidEvent],
                                blocking=True)

    for binlogevent in stream:
        if isinstance(binlogevent, MariadbGtidEvent):
            print(f"GTID: {binlogevent.gtid}")
        if isinstance(binlogevent, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)):
            print(f"GTID: {binlogevent.schema}")
            print(binlogevent.rows)
        
        binlogevent.dump()
    
    stream.close()

if __name__ == "__main__":
    main()

