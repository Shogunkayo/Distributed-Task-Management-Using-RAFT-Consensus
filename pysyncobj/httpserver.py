from http.server import BaseHTTPRequestHandler, HTTPServer
from kvstorage import KVStorage
import json
import sys
import copy
import time
import uuid
import mysql.connector
import time
from threading import Thread, Condition

_g_kvstorage = None
dbName = None
server_no = None

class KVServer(BaseHTTPRequestHandler):
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

            if self.path == "/addTask":
                try:
                    ID = metadata["ID"]
                    content = metadata["content"]

                except Exception as e:
                    print("ERROR: ", e)
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "invalid request"}).encode('utf-8'))
                    return

                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(metadata).encode("utf-8"))

                query = "INSERT INTO Persons VALUES " + content
                _g_kvstorage.update_database(query)
                _g_kvstorage.append_to_log(query)

                print("Done")

                return

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

    thread.join()