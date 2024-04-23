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
        try:
            self.db = mysql.connector.connect(host="localhost", user="root", password="nagag0410?", database=dbName)
            print("Connected to database: ", self.dbName)
        except mysql.connector.Error as e:
            print("Error connecting to the database:", e)
            self.db = None

        if self.db:
            self.cursor = self.db.cursor()
            self.query_log = []
        else:
            print("Database connection failed")

    def update_database_helper(self, query):
        self.cursor.execute(query)
        self.db.commit()

    
    def append_to_log(self, query):
        self.query_log.append(query)
        print("Length of log: ", len(self.query_log))

    
    def get_most_recent_query(self):
        return self.query_log[-1]
    

    def get_query_log(self):
        print(self.query_log)
        return self.query_log

    def update_database(self, query):
        self.update_database_helper(query)

    def get_tasks_by_user(self, user_id):
        query = "SELECT * FROM Tasks WHERE created_by = %s"
        values = (user_id,)
        self.cursor.execute(query, values)
        tasks = self.cursor.fetchall()
        task_list = []
        for task in tasks:
            task_dict = {
                "id": task[0],
                "title": task[1],
                "description": task[2],
                "status": task[3],
                "created_at": str(task[4]),  # Convert datetime to string
                "updated_at": str(task[5]),  # Convert datetime to string
                "priority": task[6],
                "created_by": task[7]
            }
            task_list.append(task_dict)
        return task_list