# 1. def(get mongodb config)
# 2. def(connect)
# 3. def(disconnect)
# 4.def(reconnect)
# 5.def(exit)

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

class MongoDBConnect:
    def __init__(self, mongo_uri, db_name):
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.client = None
        self.db = None

    def connect(self):
        try:
            self.client = MongoClient(self.mongo_uri)
            self.client.server_info() #test connection
            self.db = self.client[self.db_name]
            print(f"----------Connected to MongoDB: {self.db_name}------------")
            return self.db
        except ConnectionFailure as e:
            raise Exception(f"------------------Failed to connect MongoDB : {e}---------------------") from e

    def close(self):
        if self.client:
            self.client.close()
        print("---------------MongoDB connection closed---------------")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()