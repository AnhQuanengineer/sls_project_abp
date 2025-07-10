from database.mongodb_connect import MongoDBConnect
from database.schema_manager import create_mongodb_schema
from get_config.database_config import get_database_config
from pathlib import Path
import json


def main(config):
    # MONGODB
    with MongoDBConnect(config['mongodb'].uri, config["mongodb"].db_name) as mongo_client:
        create_mongodb_schema(mongo_client.connect())

    # MONGO_DATA = Path("/home/abp-server4/Downloads/sls_project/sls_etl_orgs2.json")
    # # MONGO_DATA = Path("/home/victo/PycharmProjects/GMV_FOR_QUAN_CRAWRL/data/menard/menard_product_productReview3.json")
    # with MongoDBConnect(config["mongodb"].uri, config["mongodb"].db_name) as mongo_client:
    #     # mongo_client.connect().drop_collection("Product_review")
    #     # mongo_client.connect().create_collection("Product_review")
    #
    #     with open(MONGO_DATA, "r") as file:
    #         data = json.load(file)
    #
    #     if isinstance(data, list):
    #         result = mongo_client.db.sls_etl_orgs.insert_many(data)
    #         print(f"Inserted {len(result.inserted_ids)} documents")
    #     else:
    #         result = mongo_client.db.sls_etl_orgs.insert_one(data)
    #         print(f"Inserted document with ID: {result.inserted_id}")

if __name__ == "__main__":
    config = get_database_config()
    main(config)