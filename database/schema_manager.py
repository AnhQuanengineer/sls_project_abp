import json
import os
from pathlib import Path
import pymongo
from psycopg2 import Error
from get_config.database_config import get_database_config

SQL_FILE_PATH = os.path.join(os.path.dirname(__file__), "../../sql/schema_postgres.sql")
SQL_FILE_PATH_CLICKHOUSE = os.path.join(os.path.dirname(__file__), "../../sql/schema_clickhouse.sql")
db_config = get_database_config()

def create_mongodb_schema(db):
    db.drop_collection("sls_not_spam_posts")
    db.create_collection("sls_not_spam_posts", validator={
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["post_id", "org_id"],
            "properties": {
                "post_id": {
                    "bsonType": "string"
                },
                "doc_type": {
                    "bsonType": ["int", "null"]
                },
                "source_type": {
                    "bsonType": ["int", "null"]
                },
                "crawl_source": {
                    "bsonType": ["int", "null"]
                },
                "crawl_source_code": {
                    "bsonType": ["string", "null"]
                },
                "pub_time": {
                    "bsonType": ["long", "null"]
                },
                "crawl_time": {
                    "bsonType": ["long", "null"]
                },
                "subject_id": {
                    "bsonType": ["string", "null"]
                },
                "title": {
                    "bsonType": ["string", "null"]
                },
                "description": {
                    "bsonType": ["string", "null"]
                },
                "content": {
                    "bsonType": ["string", "null"]
                },
                "url": {
                    "bsonType": ["string", "null"]
                },
                "media_urls": {
                    "bsonType": ["string", "null"]
                },
                "comments": {
                    "bsonType": ["int", "null"]
                },
                "shares": {
                    "bsonType": ["int", "null"]
                },
                "reactions": {
                    "bsonType": ["int", "null"]
                },
                "favors": {
                    "bsonType": ["int", "null"]
                },
                "views": {
                    "bsonType": ["int", "null"]
                },
                "web_tags": {
                    "bsonType": ["string", "null"]
                },
                "web_keywords": {
                    "bsonType": ["string", "null"]
                },
                "auth_id": {
                    "bsonType": ["string", "null"]
                },
                "auth_name": {
                    "bsonType": ["string", "null"]
                },
                "auth_type": {
                    "bsonType": ["int", "null"]
                },
                "auth_url": {
                    "bsonType": ["string", "null"]
                },
                "source_id": {
                    "bsonType": ["string", "null"]
                },
                "source_name": {
                    "bsonType": ["string", "null"]
                },
                "source_url": {
                    "bsonType": ["string", "null"]
                },
                "reply_to": {
                    "bsonType": ["string", "null"]
                },
                "level": {
                    "bsonType": ["string", "null"]
                },
                "org_id": {
                    "bsonType": "int"
                },
                "sentiment": {
                    "bsonType": "int"
                },
                "pg_sync": {
                    "bsonType": "bool"
                },
                "createdAt": {
                    "bsonType": "date"
                },
                "updatedAt": {
                    "bsonType": "date"
                }
            }
        }
    })
    # db.sls_not_spam_posts.create_index("_id",unique = True)

def create_postgres_schema(connection, cursor):
    schema = db_config["postgres"].schema
    # connection.autocommit = True
    # cursor.execute(f"DROP DATABASE IF EXISTS {database}")
    # cursor.execute(f"CREATE DATABASE {database}")
    cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};;")
    cursor.execute(f"SET search_path TO {schema};;")

    connection.commit()
    print(f"------------Create schema {schema} in Postgres---------------")
    try:
        with open(SQL_FILE_PATH, "r") as file:
            sql_script = file.read()
            sql_commands = [cmd.strip() for cmd in sql_script.split(";") if cmd.strip()]
            for cmd in sql_commands:
                cursor.execute(cmd)
                print(f"--------------Execute: {cmd.strip()[:50]}-----------------")
            connection.commit()
            print("-------------Create Postgres schema-----------------")
    except Error as e:
        connection.rollback()
        print(f"-------------------Can't execute to sql command: {e}--------------------")

def validate_postgres_schema(cursor):
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'shopee_data'
    """)
    tables = [row[0] for row in cursor.fetchall()]
    # print(tables)
    if "products" not in tables:
        raise ValueError("-----------------Missing tables in Postgres------------------")
    cursor.execute("SELECT * FROM products WHERE product_id = 29569685929")
    user = cursor.fetchone()
    if not user:
        raise ValueError("-------------products not found-------------------")
    print("--------------Validated schema in Postgres-----------------------")


def create_clickhouse_schema(client):
    database = db_config["clickhouse"].database
    client.execute(f"DROP DATABASE IF EXISTS {database}")
    client.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    client.execute(f"USE {database}")
    print(f"------------Create database {database} in ClickHouse---------------")

    try:
        with open(SQL_FILE_PATH_CLICKHOUSE, "r") as file:
            sql_script = file.read()
            sql_commands = [cmd.strip() for cmd in sql_script.split(";") if cmd.strip()]

            for cmd in sql_commands:
                client.execute(cmd)
                print(f"----------------------Executed: {cmd.strip()[:50]}---------------------")
            print("--------------Created ClickHouse schema---------------------")

    except Error as e:
        print(f"-------------------Can't execute to sql command: {e}--------------------")

def validate_clickhouse_schema(client):
    result = client.execute(
    """
        SELECT name 
        FROM system.tables
        WHERE database = 'shopee_data'
    """)
    tables = [row[0] for row in result]
    # print(tables)
    if "products" not in tables:
        raise ValueError("---------------------Missing table in Click House---------------")

    result = client.execute(
        """
        SELECT *
        FROM products
        WHERE product_id = 1
        """
    )

    record = result[0] if result else None

    if not record:
        raise ValueError("---------------product not found-------------")

    print("-------------Validated schema in Click House------------------")

