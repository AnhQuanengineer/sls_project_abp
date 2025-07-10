import os
from dataclasses import dataclass
from dotenv import load_dotenv
from typing import Dict


class DatabaseConfig():
    def validate(self):
        for key,value in self.__dict__.items():
            if value is None:
                raise ValueError(f"-------Missing config for {key}------------")

@dataclass
class MongoDBConfig(DatabaseConfig):
    uri : str
    db_name : str
    collection_name_key_words: str
    collection_name_posts: str

@dataclass
class PostgresConfig(DatabaseConfig):
    host : str
    user : str
    password : str
    port : str
    schema: str
    database : str
    driver: str
    table_posts: str

# @dataclass
# class ClickhouseConfig(DatabaseConfig):
#     host : str
#     user : str
#     password : str
#     port : str
#     database : str
#     port_spark : str
#     driver : str

@dataclass
class ElsConfig(DatabaseConfig):
    host : str
    port : str
    user: str
    password: str
    index: str

def get_database_config() -> Dict[str,DatabaseConfig]:
    load_dotenv()
    config = {
        "mongodb" : MongoDBConfig(
            uri = os.getenv("MONGO_INITDB_URI"),
            db_name = os.getenv("MONGO_INITDB_DATABASE"),
            collection_name_key_words=os.getenv("MONGO_COLLECTION_NAME_KEY_WORDS"),
            collection_name_posts=os.getenv("MONGO_COLLECTION_NAME_POSTS")
        ),
        "postgres" : PostgresConfig(
            host= os.getenv("POSTGRES_STAGING_HOST"),
            port= os.getenv("POSTGRES_STAGING_PORT"),
            user= os.getenv("POSTGRES_STAGING_USER"),
            password= os.getenv("POSTGRES_STAGING_PASSWORD"),
            database= os.getenv("POSTGRES_STAGING_DB"),
            schema = os.getenv("POSTGRES_STAGING_SCHEMA"),
            driver= os.getenv("POSTGRES_STAGING_DRIVER"),
            table_posts= os.getenv("POSTGRES_STAGING_TABLE_POSTS")
        ),
        # "clickhouse" : ClickhouseConfig(
        #     host= os.getenv("CLICKHOUSE_HOST"),
        #     port= os.getenv("CLICKHOUSE_PORT_TCP"),
        #     user= os.getenv("CLICKHOUSE_USER"),
        #     password= os.getenv("CLICKHOUSE_PASSWORD"),
        #     database= os.getenv("CLICKHOUSE_DATABASE"),
        #     port_spark= os.getenv("CLICKHOUSE_PORT_HTTP"),
        #     driver= os.getenv("CLICKHOUSE_DRIVER")
        # )
        "elastic_search" : ElsConfig(
            host= os.getenv("ELASTIC_HOST"),
            port= os.getenv("ELASTIC_PORT"),
            user= os.getenv("ELASTIC_AUTH_USERNAME"),
            password= os.getenv("ELASTIC_AUTH_PASSWORD"),
            index= os.getenv("ELASTIC_RAW_POSTS_INDEX")
        )
    }

    for db, setting in config.items():
        setting.validate()

    return config

db_config = get_database_config()

# print(db_config)