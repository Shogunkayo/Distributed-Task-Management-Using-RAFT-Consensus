from pysyncobj_modified import SyncObj, SyncObjConf, replicated

class KVStorage(SyncObj):
    def __init__(self, selfAddress, partnerAddrs, dumpFile, journal):
        conf = SyncObjConf(
            fullDumpFile=dumpFile,
            journalFile=journal
        )
        super(KVStorage, self).__init__(selfAddress, partnerAddrs, conf)

        self.query_log = []

    @replicated
    def append_to_log(self, query):
        self.query_log.append(query)

    @replicated
    def get_most_recent_query(self):
        return self.query_log[len(self.query_log) - 1]
    
    @replicated
    def get_query_log(self):
        return self.query_log