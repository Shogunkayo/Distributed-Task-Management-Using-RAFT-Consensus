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
            self.db = mysql.connector.connect(host="localhost", user="ganesh", password="123", database=dbName)
            print("Connected to database: ", self.dbName)

        except mysql.connector.Error as e:
            print("Error connecting to the database:", e)
            self.db = None

        if self.db:
            self.cursor = self.db.cursor()

            self.cursor.execute("DELETE FROM Task_Assign")
            self.db.commit()

            self.cursor.execute("DELETE FROM Tasks")
            self.db.commit()

            self.cursor.execute("DELETE FROM Users")
            self.db.commit()

            self.query_log = []
        else:
            print("Database connection failed")

    def update_database_helper(self, query):
        self.cursor.execute(query)
        self.db.commit()

    def modify_task_helper(self, taskid, title, description, status, priority, updated_at):
        # taskid = '"' + taskid + '"'
        # title = '"' + taskid + '"'
        # description = '"' + description + '"'

        print("before")

        query1 = "UPDATE Tasks" + " SET title = " + title + " WHERE task_id = " + taskid
        query2 = "UPDATE Tasks" + " SET description = " + description + " WHERE task_id = " + taskid
        query3 = "UPDATE Tasks" + " SET status = " + status + " WHERE task_id = " + taskid
        query4 = "UPDATE Tasks" + " SET priority = " + str(priority) + " WHERE task_id = " + taskid
        query5 = "UPDATE Tasks" + " SET updated_at = " + updated_at + " WHERE task_id = " + taskid

        self.cursor.execute(query1)
        self.db.commit()

        self.cursor.execute(query2)
        self.db.commit()

        self.cursor.execute(query3)
        self.db.commit()

        self.cursor.execute(query4)
        self.db.commit()

        self.cursor.execute(query5)
        self.db.commit()

        print("after")

    @replicated    
    def append_to_log(self, query):
        self.query_log.append(query)
        print("Length of log: ", len(self.query_log))

    @replicated    
    def get_most_recent_query(self):
        return self.query_log[-1]
    
    @replicated    
    def get_query_log(self):
        print(self.query_log)
        return self.query_log

    @replicated    
    def update_database(self, query):
        self.update_database_helper(query)

    @replicated
    def modify_task(self, taskid, title, description, status, priority, updated_at):
        self.modify_task_helper(taskid, title, description, status, priority, updated_at)

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