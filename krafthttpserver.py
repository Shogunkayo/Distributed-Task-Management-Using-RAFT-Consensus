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

            if path_parts[0] == "getActiveBrokers":
                records = _g_kvstorage.read(record_type="RegisterBrokerRecord")
                active_brokers = [broker for broker in records if broker["fields"]["brokerStatus"] == "ALIVE"]
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(active_brokers).encode("utf-8"))
                return

            # Request of format "RegisterBrokerRecord/<brokerId>"
            elif path_parts[0] == "getBrokerById":
                try:
                    broker_id = int(path_parts[1])
                except Exception as e:
                    print("ERROR: ", e)
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "Invalid broker id"}).encode("utf-8"))
                    return

                record = _g_kvstorage.read(record_type="RegisterBrokerRecord", unique_field="brokerId", unique_value=broker_id)

                if record == {}:
                    self.send_response(404)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "Broker id does not exist"}).encode("utf-8"))
                    return

                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(record).encode("utf-8"))
                return

            elif path_parts[0] == "getTopicByName":
                try:
                    topic_name = path_parts[1]
                except Exception as e:
                    print("ERROR: ", e)
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "Invalid topic name"}).encode("utf-8"))
                    return

                record = _g_kvstorage.read(record_type="TopicRecord", unique_field="name", unique_value=topic_name)

                if record == {}:
                    self.send_response(404)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "No topics found"}).encode("utf-8"))
                    return

                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(record).encode("utf-8"))
                return

            elif path_parts[0] == "brokerMgmt":
                try:
                    prev_timestamp = int(path_parts[1])
                except ValueError as e:
                    print("ERROR: ", e)
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "Invalid timestamp"}).encode("utf-8"))
                    return

                if int(time.time()) - prev_timestamp > 600:
                    # more than 10 mins
                    event_log = _g_kvstorage.event_log
                else:
                    # within 10mins
                    event_log = _g_kvstorage.get_metadata_updates(prev_timestamp)

                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(event_log).encode("utf-8"))
                return

            elif path_parts[0] == "clientMgmt":
                try:
                    prev_timestamp = int(path_parts[1])
                except ValueError as e:
                    print("ERROR: ", e)
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "Invalid timestamp"}).encode("utf-8"))
                    return

                current_time = int(time.time())
                ten_minutes_ago = current_time - 600
                if prev_timestamp < ten_minutes_ago:
                    # more than 10 mins
                    event_log = _g_kvstorage.event_log
                else:
                    # within 10mins
                    event_log = _g_kvstorage.get_metadata_updates(prev_timestamp)

                required_records = ["RegisterBrokerRecord", "TopicRecord", "PartitionRecord"]
                required_event_log = [log for log in event_log if log["record_type"] in required_records]

                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(required_event_log).encode("utf-8"))
                return

            elif path_parts[0] == "read":
                try:
                    record_type = path_parts[1]
                    records = _g_kvstorage.read(record_type=record_type)
                except Exception as e:
                    print("ERROR: ", e)
                    records = _g_kvstorage.read()

                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(records).encode("utf-8"))
                return

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

            if self.path == "/registerBroker":
                # Body must have json file with broker details
                try:
                    if metadata["name"] != "RegisterBrokerRecord":
                        raise
                    brokerHost = metadata["fields"]["brokerHost"]
                    brokerPort = metadata["fields"]["brokerPort"]
                    brokerId = metadata["fields"]["brokerId"]
                    _ = metadata["fields"]["securityProtocol"]
                    _ = metadata["fields"]["rackId"]
                    timestamp = int(metadata["timestamp"])
                    all_records = _g_kvstorage.read(metadata["name"])
                except Exception as e:
                    print("ERROR: ", e)
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "invalid request"}).encode('utf-8'))
                    return

                if _g_kvstorage.metadata_store[metadata["name"]]["timestamp"] >= timestamp:
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "stale request"}).encode('utf-8'))
                    return

                for record in all_records:
                    if record["fields"]["brokerHost"] == brokerHost and record["fields"]["brokerPort"] == brokerPort:
                        self.send_response(400)
                        self.send_header("Content-type", "application/json")
                        self.end_headers()
                        self.wfile.write(json.dumps({"error": "socket in use"}).encode('utf-8'))
                        return

                    if record["fields"]["brokerId"] == brokerId:
                        self.send_response(400)
                        self.send_header("Content-type", "application/json")
                        self.end_headers()
                        self.wfile.write(json.dumps({"error": "brokerId already exists"}).encode('utf-8'))
                        return

                metadata["fields"]["brokerStatus"] = "ALIVE"
                metadata["fields"]["epoch"] = 0
                metadata["fields"]["internalUUID"] = str(uuid.uuid1())

                _g_kvstorage.create(metadata, timestamp)
                _g_kvstorage.append_to_log(operation="create", record_type="RegisterBrokerRecord", data=metadata, timestamp=timestamp)
                self.send_response(201)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(metadata).encode("utf-8"))
                return

            elif self.path == "/createTopic":
                try:
                    if metadata["name"] != "TopicRecord":
                        raise
                    topicName = metadata["fields"]["name"]
                    timestamp = int(metadata["timestamp"])
                    all_records = _g_kvstorage.read(record_type=metadata["name"])
                except Exception as e:
                    print("ERROR: ", e)
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "invalid request"}).encode('utf-8'))
                    return

                if _g_kvstorage.metadata_store[metadata["name"]]["timestamp"] >= timestamp:
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "stale request"}).encode('utf-8'))
                    return

                for record in all_records:
                    if record["fields"]["name"] == topicName:
                        self.send_response(400)
                        self.send_header("Content-type", "application/json")
                        self.end_headers()
                        self.wfile.write(json.dumps({"error": "topic name already exists"}).encode('utf-8'))
                        return

                metadata["fields"]["topicUUID"] = str(uuid.uuid1())

                _g_kvstorage.create(metadata, timestamp)
                _g_kvstorage.append_to_log(operation="create", record_type="TopicRecord", data=metadata, timestamp=timestamp)
                self.send_response(201)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(metadata).encode("utf-8"))
                return

            elif self.path == "/createPartition":
                try:
                    if metadata["name"] != "PartitionRecord":
                        raise
                    partitionId = metadata["fields"]["partitionId"]
                    topicId = metadata["fields"]["topicUUID"]
                    _ = metadata["fields"]["replicas"]
                    _ = metadata["fields"]["ISR"]
                    brokerUUID = metadata["fields"]["leader"]
                    topic_record = _g_kvstorage.read(record_type="TopicRecord", unique_field="topicUUID", unique_value=topicId)
                    broker_leader = _g_kvstorage.read(record_type="RegisterBrokerRecord", unique_field="internalUUID", unique_value=brokerUUID)
                    partition_record = _g_kvstorage.read(record_type="PartitionRecord", unique_field="partitionId", unique_value=partitionId)
                    timestamp = int(metadata["timestamp"])

                except Exception as e:
                    print("ERROR: ", e)
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "invalid request"}).encode('utf-8'))
                    return

                if _g_kvstorage.metadata_store[metadata["name"]]["timestamp"] >= timestamp:
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "stale request"}).encode('utf-8'))
                    return

                if topic_record == {}:
                    self.send_response(404)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "topicUUID does not exist"}).encode("utf-8"))
                    return

                if broker_leader == {}:
                    self.send_response(404)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "leader does not exist"}).encode("utf-8"))
                    return

                if partition_record != {}:
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "partitionId already exists"}).encode("utf-8"))
                    return

                metadata["fields"]["addingReplicas"] = []
                metadata["fields"]["removingReplicas"] = []
                metadata["fields"]["partitionEpoch"] = 0
                _g_kvstorage.create(metadata, timestamp)
                _g_kvstorage.append_to_log(operation="create", record_type="PartitionRecord", data=metadata, timestamp=timestamp)
                self.send_response(201)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(metadata).encode("utf-8"))
                return

            elif self.path == "/createProducer":
                try:
                    if metadata["name"] != "ProducerIdsRecord":
                        raise
                    brokerId = metadata["fields"]["brokerId"]
                    timestamp = metadata["timestamp"]
                    producerId = metadata["fields"]["producerId"]
                    broker_record = _g_kvstorage.read(record_type="RegisterBrokerRecord", unique_field="internalUUID", unique_value=brokerId)
                    producer_record = _g_kvstorage.read(record_type="ProducerIdsRecord", unique_field="producerId", unique_value=producerId)
                except Exception as e:
                    print("ERROR: ", e)
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "invalid request"}).encode('utf-8'))
                    return

                if producer_record != {}:
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "producerId already exists"}).encode('utf-8'))
                    return

                if broker_record == {}:
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "brokerID does not exist"}).encode('utf-8'))
                    return

                if broker_record["fields"]["brokerStatus"] != "ALIVE":
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "broker is not alive"}).encode('utf-8'))
                    return

                if _g_kvstorage.metadata_store[metadata["name"]]["timestamp"] >= int(timestamp):
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "stale request"}).encode('utf-8'))
                    return

                metadata["fields"]["brokerEpoch"] = broker_record["fields"]["epoch"]
                _g_kvstorage.create(metadata, int(timestamp))
                _g_kvstorage.append_to_log(operation="create", record_type="ProducerIdsRecord", data=metadata, timestamp=timestamp)
                self.send_response(201)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(metadata).encode("utf-8"))
                return

            elif self.path == "/updateBroker":
                try:
                    if metadata["name"] != "RegistrationChangeBrokerRecord":
                        raise
                    brokerId = metadata["fields"]["brokerId"]
                    timestamp = int(metadata["timestamp"])
                    broker_record = _g_kvstorage.read(record_type="RegisterBrokerRecord", unique_field="brokerId", unique_value=brokerId)
                except Exception as e:
                    print("ERROR: ", e)
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "invalid request"}).encode('utf-8'))
                    return

                if _g_kvstorage.metadata_store[metadata["name"]]["timestamp"] >= timestamp:
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "stale request"}).encode('utf-8'))
                    return

                if broker_record == {}:
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "brokerID does not exist"}).encode('utf-8'))
                    return

                log_data = {
                    "type": "metadata",
                    "name": "RegisterBrokerRecord",
                    "fields_old": {"epoch": broker_record["fields"]["epoch"]},
                    "fields_new": {},
                    "timestamp": timestamp
                }

                updated_record = copy.deepcopy(broker_record)
                if "brokerStatus" in metadata["fields"] and metadata["fields"]["brokerStatus"] == "CLOSED":
                    updated_record["fields"]["brokerStatus"] = "CLOSED"
                    log_data["fields_old"]["brokerStatus"] = broker_record["fields"]["brokerStatus"]
                    log_data["fields_new"]["brokerStatus"] = "CLOSED"
                else:
                    for key, value in metadata["fields"].items():
                        if key == "brokerId" or key == "epoch":
                            continue
                        try:
                            old_value = updated_record["fields"][key]
                            updated_record["fields"][key] = value
                            log_data["fields_old"][key] = old_value
                            log_data["fields_new"][key] = value
                        except Exception as e:
                            print("ERROR: ", e)
                            self.send_response(400)
                            self.send_header("Content-type", "application/json")
                            self.end_headers()
                            self.wfile.write(json.dumps({"error": "invalid field in request"}).encode('utf-8'))
                            return

                    updated_record["fields"]["epoch"] += 1

                updated_record["timestamp"] = timestamp
                metadata["fields"]["epoch"] = updated_record["fields"]["epoch"]
                log_data["fields_new"]["epoch"] = updated_record["fields"]["epoch"]

                _g_kvstorage.delete_record(record_type="RegisterBrokerRecord", unique_id=brokerId, timestamp=timestamp)
                _g_kvstorage.create(updated_record, timestamp)
                _g_kvstorage.append_to_log(operation="update", record_type="RegisterBrokerRecord", data=log_data, timestamp=timestamp)
                _g_kvstorage.create(metadata, timestamp)
                _g_kvstorage.append_to_log(operation="create", record_type="RegistrationChangeBrokerRecord", data=metadata, timestamp=timestamp)

                self.send_response(201)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(updated_record).encode("utf-8"))
                return

            elif self.path == "/delete":
                try:
                    record_type = metadata["record_type"]
                    unique_id = metadata["unique_id"]
                    timestamp = int(metadata["timestamp"])
                except Exception as e:
                    print("ERROR: ", e)
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "invalid request"}).encode('utf-8'))
                    return

                if (record_type != "RegisterBrokerRecord") and (record_type != "TopicRecord"):
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "invalid record_type"}).encode('utf-8'))
                    return

                if _g_kvstorage.metadata_store[record_type]["timestamp"] > timestamp:
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "stale request"}).encode('utf-8'))
                    return

                _g_kvstorage.delete_record(record_type, unique_id, timestamp)
                _g_kvstorage.append_to_log(operation="delete", record_type=record_type, data=metadata, timestamp=timestamp)
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
