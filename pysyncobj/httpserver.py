from http.server import BaseHTTPRequestHandler, HTTPServer
from kvstorage import KVStorage
import json
import sys
import copy
import time
import uuid

_g_kvstorage = None

class KVServer(BaseHTTPRequestHandler):
    def do_GET(self):
        '''
        Read from urls
        '''
        global _g_kvstorage
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
        if _g_kvstorage is not None:
            content_length = int(self.headers.get('content-length'))
            metadata = self.rfile.read(content_length).decode("utf-8")
            metadata = json.loads(metadata)

            if self.path == "/sendQuery":
                try:
                    queryID = metadata["queryID"]
                    query = metadata["query"]

                except Exception as e:
                    print("ERROR: ", e)
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "invalid request"}).encode('utf-8'))
                    return

                _g_kvstorage.append_to_log(query = metadata)
                self.send_response(201)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(metadata).encode("utf-8"))
                return

            else:
                self.send_response(400)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Invalid URL"}).encode("utf-8"))
                return

if __name__ == "__main__":
    if len(sys.argv) < 6:
        print('Usage: %s http_port dump_file.bin journal_file selfHost:port partner1Host:port partner2Host:port ...' % sys.argv[0])
        sys.exit(-1)

    http_port = int(sys.argv[1])
    dumpFile = sys.argv[2]
    journal = sys.argv[3]
    selfAddr = sys.argv[4]
    partners = sys.argv[5:]

    _g_kvstorage = KVStorage(selfAddr, partners, dumpFile, journal)
    httpServer = HTTPServer((selfAddr.split(":")[0], http_port), KVServer)
    httpServer.serve_forever()
