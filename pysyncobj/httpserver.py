from http.server import BaseHTTPRequestHandler, HTTPServer
from kvstorage import KVStorage
import json
import sys
import copy
import time
import datetime
import uuid
import mysql.connector
import time
from threading import Thread, Condition

_g_kvstorage = None
dbName = None
server_no = None

class KVServer(BaseHTTPRequestHandler):

    def _set_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self._set_headers()

    def do_GET(self):
        '''
        Read from urls
        '''
        global _g_kvstorage
        global dbName

        if _g_kvstorage is not None:
            path_parts = self.path.split("/")[1:]

            if path_parts[0] == "getMostRecentQuery":
                record = _g_kvstorage.get_most_recent_query()
                print(record)

            elif path_parts[0] == "getQueryLog":
                query_log = _g_kvstorage.get_query_log()
                print(query_log)

            elif path_parts[0] == "getTasksByUser":
                try:
                    user_id = int(path_parts[1])  # Assuming user ID is passed in the URL
                except ValueError:
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "Invalid user ID"}).encode("utf-8"))
                    return

                tasks = _g_kvstorage.get_tasks_by_user(user_id)  # Implement this method in KVStorage
                if tasks is not None:
                    self.send_response(200, tasks)
                    self.send_header("Content-type", "application/json")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    self.wfile.write(json.dumps(tasks).encode("utf-8"))
                else:
                    self.send_response(404)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "User not found or has no tasks"}).encode("utf-8"))


            else:
                self.send_response(400)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Invalid URL"}).encode("utf-8"))
                return

    def do_POST(self):
        '''
        Post to urls
        '''
        global _g_kvstorage
        global dbName

        if _g_kvstorage is not None:
            content_length = int(self.headers.get('content-length'))
            metadata = self.rfile.read(content_length).decode("utf-8")
            metadata = json.loads(metadata)

            if self.path == "/addUser":
                try:
                    userid = metadata["userid"]
                    username = metadata["username"]
                    email = metadata["email"]
                    password = metadata["password"]

                except Exception as e:
                    print("ERROR: ", e)
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "invalid request"}).encode('utf-8'))
                    return

                content = "(" + str(userid) + "," + "\'"+ username + "\'" + "," + "\'" + email + "\'"+ "," + "\'"+ password +"\'"+ ");"
                print(content)

                query = "INSERT INTO Users (user_id, username, email, password) VALUES " + content 
                _g_kvstorage.update_database(query)
                _g_kvstorage.append_to_log(query)

                print("Done")
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                response_data = {"message":"User added successfully"}
                self.wfile.write(json.dumps(response_data).encode("utf-8"))
                return
            


            elif self.path == "/addTask":
                try:
                    taskid = '"' + metadata["taskid"] + '"'
                    title = '"' + metadata["title"] + '"'
                    description = '"' + metadata["description"] + '"'
                    status = '"' + metadata["status"] + '"'
                    priority = metadata["priority"]
                    created_by = metadata["created_by"]
                    
                    # Set created_at and updated_at
                    created_at = '"' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '"'
                    updated_at = created_at

                except KeyError as e:
                    print("ERROR: Missing key in request data:", e)
                    self.send_error(400, "Invalid request data")
                    return

                # Construct SQL query
                query = "INSERT INTO Tasks (task_id, title, description, status, created_at, updated_at, priority, created_by) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                values = (taskid, title, description, status, created_at, updated_at, priority, created_by)

                # Execute query and append to log
                print(query % values)
                _g_kvstorage.update_database(query % values)
                _g_kvstorage.append_to_log(query % values)

                # Send success response
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                response_data = {"message": "Task added successfully"}
                self.wfile.write(json.dumps(response_data).encode("utf-8"))

            elif self.path == "/checkUser":
                print("Hello, CheckUser running...")
                try:
                    username = '"' + metadata["username"] +'"'
                    password = '"' + metadata["password"] + '"'

                except KeyError as e:
                    print("ERROR: Missing key in request data:", e)
                    self.send_error(400, "Invalid request data")
                    return

                # Construct SQL query to check if the user exists
                query = "SELECT user_id FROM Users WHERE username = %s AND password = %s"
                values = (username, password)

                # Execute the query
                _g_kvstorage.cursor.execute(query % values)
                result = _g_kvstorage.cursor.fetchone()

                for _ in _g_kvstorage.cursor:
                    pass

                # Check if user exists based on query result
                if result:
                    print("executing")
                    self.send_response(200)
                    # User exists and return ID
                    user_id = result[0]
                    self.send_header("Content-type", "application/json")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    response_data = {"exists": True, "userId": user_id}
                    
                else:
                    # User does not exist
                    self.send_response(404)
                    self.send_header("Content-type", "application/json")
                    response_data = {"exists": False}

                self.end_headers()
                self.wfile.write(json.dumps(response_data).encode("utf-8"))

            # elif self.path == "/updateTask":
            #     try:
            #         title = '"' + metadata["title"] + '"'
            #         description = '"' + metadata["description"] + '"'
            #         status = '"' + metadata["status"] + '"'
            #         priority = metadata["priority"]
            #         created_by = metadata["created_by"]
                
            else:
                self.send_response(400)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Invalid URL"}).encode("utf-8"))
                return
            
if __name__ == "__main__":
    if len(sys.argv) < 8:
        print('Usage: %s http_port dump_file.bin journal_file selfHost:port partner1Host:port partner2Host:port ...' % sys.argv[0])
        sys.exit(-1)

    http_port = int(sys.argv[1])
    dbName = sys.argv[2]
    server_no = int(sys.argv[3])
    dumpFile = sys.argv[4]
    journal = sys.argv[5]
    selfAddr = sys.argv[6]
    partners = sys.argv[7:]

    _g_kvstorage = KVStorage(selfAddr, partners, dumpFile, journal, server_no, dbName)
    httpServer = HTTPServer((selfAddr.split(":")[0], http_port), KVServer)

    httpServer.serve_forever()