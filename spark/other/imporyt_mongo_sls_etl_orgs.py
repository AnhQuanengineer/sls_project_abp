from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
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

    schema_keywords = StructType([
        StructField("code", StringType(), True),
        StructField("keywords", StringType(), True),
        StructField("org_id", IntegerType(), True),
        StructField("spamwords", StringType(), True),
    ])

    spark_configs = get_spark_config()

    orgs_df = spark_connect.spark.read.format("mongo") \
        .option("uri", spark_configs["mongodb"]["uri"]) \
        .option("database", spark_configs["mongodb"]["database"]) \
        .option("collection", "sls_not_spam_posts") \
        .load()

    orgs_df= orgs_df.select(col("post_id"))

#     # orgs_df = spark_connect.spark.read.format("json") \
#     #     .schema(schema_keywords) \
#     #     .load("/home/abp-server4/Downloads/sls_project/sls_etl_orgs2.json")
#
#     orgs_df1 = orgs_df.withColumn(
#     "keywords",
#     split(regexp_replace(col("keywords"), r"[\[\]]", ""), ",")
# )
#     orgs_df1 = orgs_df1.withColumn(
#         "spamwords",
#         split(
#             # Loại bỏ dấu [ và ] ở đầu và cuối
#             regexp_replace(col("spamwords"), r"^\[|\]$", ""),
#             # Tách dựa trên "}, {"
#             r"\}, \{"
#         )
#     )
#
#     # Làm sạch từng phần tử trong mảng (loại bỏ khoảng trắng dư thừa nếu có)
#     orgs_df1 = orgs_df1.withColumn(
#         "spamwords",
#         expr("transform(spamwords, x -> trim(x))")
#     )

    orgs_df.show()
    # orgs_df1.printSchema()
if __name__ == "__main__":
    main()