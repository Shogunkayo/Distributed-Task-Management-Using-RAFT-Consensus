from pysyncobj_modified import SyncObj, SyncObjConf, replicated

class KVStorage(SyncObj):
    def __init__(self, selfAddress, partnerAddrs, dumpFile, journal):
        conf = SyncObjConf(
            fullDumpFile=dumpFile,
            journalFile=journal
        )
        super(KVStorage, self).__init__(selfAddress, partnerAddrs, conf)
        self.metadata_store = {
            "RegisterBrokerRecord": {
                "records": [],
                "timestamp": -1
            },

            "TopicRecord": {
                "records": [],
                "timestamp": -1,
            },

            "PartitionRecord": {
                "records": [],
                "timestamp": -1,
            },

            "ProducerIdsRecord": {
                "records": [],
                "timestamp": -1,
            },

            "RegistrationChangeBrokerRecord": {
                "records": [],
                "timestamp": -1,
            },
        }

        self.event_log = []

    @replicated
    def append_to_log(self, operation, record_type, data, timestamp):
        event = {
            "operation": operation,
            "record_type": record_type,
            "data": data,
            "timestamp": timestamp
        }
        self.event_log.append(event)

    @replicated
    def create(self, metadata, timestamp):
        self.metadata_store[metadata["name"]]["records"].append(metadata)
        self.metadata_store[metadata["name"]]["timestamp"] = int(timestamp)

    @replicated
    def delete_record(self, record_type, unique_id, timestamp):
        records = self.metadata_store[record_type]["records"]
        deleted_record = None

        for record in records:
            if record_type == 'RegisterBrokerRecord' and unique_id == record['fields']['brokerId']:
                deleted_record = record
                records.remove(record)
                self.metadata_store["RegisterBrokerRecord"]["timestamp"] = timestamp
                break
            elif record_type == 'TopicRecord' and unique_id == record['fields']['topicUUID']:
                deleted_record = record
                records.remove(record)
                self.metadata_store["TopicRecord"]["timestamp"] = timestamp
                break

        return deleted_record

    def read(self, record_type=None, unique_field=None, unique_value=None):
        if not record_type:
            return self.metadata_store
        elif not unique_field or not unique_value:
            return self.metadata_store[record_type]["records"]
        else:
            record = {}
            for r in self.metadata_store[record_type]["records"]:
                if r["fields"][unique_field] == unique_value:
                    record = r
                    break
            return record

    def get_metadata_updates(self, last_offset_timestamp):
        last_offset_index = self.find_offset_index(last_offset_timestamp)
        changes_since_offset = self.event_log[last_offset_index:]
        return changes_since_offset

    def find_offset_index(self, last_offset_timestamp):
        timestamps = [event['timestamp'] for event in self.event_log]
        last_offset_index = self.binary_search(timestamps, last_offset_timestamp)
        return max(0, last_offset_index)

    def binary_search(self, timestamps, target):
        left, right = 0, len(timestamps) - 1

        while left <= right:
            mid = (left + right) // 2

            if timestamps[mid] == target:
                return mid
            elif timestamps[mid] < target:
                left = mid + 1
            else:
                right = mid - 1

        return right  # Index of the closest entry with a timestamp less than or equal to the target
