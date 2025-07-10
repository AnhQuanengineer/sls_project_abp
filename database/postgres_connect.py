# step 1: def (get mongo config)
# step 2: def(connect)
# step 3: def(disconnect)
# step 4: def(reconnect)
# step 5: def(exit)

import psycopg2
from psycopg2 import Error

class PostgresConnect:
    def __init__(self, host, port, user, password, database):
        self.config = {"host": host, "port": port, "user": user, "password": password, "dbname": database}
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(**self.config)
            self.cursor = self.connection.cursor()
            print("-------------Connected to Postgres----------------")
            return self.cursor, self.connection
        except Error as e:
            raise Exception(f"-------------Can't connect to Postgres: {e}----------------") from e

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.closed == 0:
            self.connection.close()
        print("-------------Postgres connection close--------------------")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()