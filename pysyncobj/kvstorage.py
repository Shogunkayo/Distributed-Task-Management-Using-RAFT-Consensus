from pysyncobj_modified import SyncObj, SyncObjConf, replicated
import mysql.connector

class KVStorage(SyncObj):
    def __init__(self, selfAddress, partnerAddrs, dumpFile, journal, server_no, dbName):
        conf = SyncObjConf(
            fullDumpFile=dumpFile,
            journalFile=journal
        )
        super(KVStorage, self).__init__(selfAddress, partnerAddrs, conf)

        self.server_no = server_no
        self.dbName = dbName
        self.db = mysql.connector.connect(host = "localhost", user = "ganesh", password = "123", database = dbName)
        self.cursor = self.db.cursor()

        self.cursor.execute("DELETE FROM Tasks")
        self.db.commit()

        self.cursor.execute("DELETE FROM Users")
        self.db.commit()

        self.cursor.execute("DELETE FROM Task_Assign")
        self.db.commit()

        self.query_log = []

    def update_database_helper(self, query):
        self.cursor.execute(query)
        self.db.commit()

    def get_query_log(self):
        print(self.query_log)
        return self.query_log

    @replicated
    def append_to_log(self, query):
        self.query_log.append(query)
        print("Length of log: ", len(self.query_log))

    @replicated
    def get_most_recent_query(self):
        return self.query_log[-1]

    @replicated
    def update_database(self, query):
        self.update_database_helper(query)