from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import sys
import os
os.environ["PYSPARK_PYTHON"] = "/home/abp-server4/anaconda3/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/home/abp-server4/anaconda3/bin/python3"
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from get_config.spark_config import SparkConnect, get_spark_config
from spark.spark_write_data import SparkWriteDatabases
from datetime import datetime, timedelta
from time import mktime


def main():

    file_jar_package = [
            "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
            "org.postgresql:postgresql:42.7.4",
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.10.4"
        ]

    # Khởi tạo Spark Session
    spark_connect = SparkConnect(
        app_name="apb"
        , master_url="local[*]"
        , executor_memory="8g"
        , executor_cores=4
        , driver_memory="4g"
        , num_executors=1
        , jars=None
        , jar_packages=file_jar_package
        , spark_conf=None
        , log_level="INFO"
    )

    spark_configs = get_spark_config()

    schema_keywords = StructType([
        StructField("code", StringType(), True),
        StructField("keywords", ArrayType(StringType()), True),
        StructField("org_id", IntegerType(), True),
        StructField("org_name", StringType(), True),
        StructField("spamwords", ArrayType(StructType([
        StructField("match_phrase", StructType([
            StructField("content", StringType())
        ]))
    ])), True),
    ])

    # read key word
    orgs_df = spark_connect.spark.read.format("mongo") \
        .option("uri", "mongodb://root:tRuGz%3Dz_m*7-egJlfaEB@103.97.125.64:5525") \
        .option("database", "abp_warehouse") \
        .option("collection", "sls_etl_orgs") \
        .schema(schema_keywords) \
        .load()

    orgs_df.show(truncate= False)
    df_write = SparkWriteDatabases(spark_connect.spark, spark_configs)
    df_write.spark_write_mongodb(orgs_df, spark_configs["mongodb"]["database"],
                                                 "sls_etl_orgs", spark_configs["mongodb"]["uri"], "append")


if __name__ == "__main__":
    main()