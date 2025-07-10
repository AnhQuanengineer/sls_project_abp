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
import pytz


def main():

    file_jar_package = [
            "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
            "org.postgresql:postgresql:42.7.4",
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.10.4"
        ]

    spark_conf = {
        "spark.sql.session.timeZone": "UTC"
    }

    schema = StructType([
        StructField("doc_type", IntegerType(), True),
        StructField("source_type", IntegerType(), True),
        StructField("crawl_source", IntegerType(), True),
        StructField("crawl_source_code", StringType(), True),
        StructField("pub_time", LongType(), True),
        StructField("crawl_time", LongType(), True),
        StructField("subject_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("content", StringType(), True),
        StructField("url", StringType(), True),
        StructField("media_urls", StringType(), True),
        StructField("comments", IntegerType(), True),
        StructField("shares", IntegerType(), True),
        StructField("reactions", IntegerType(), True),
        StructField("favors", IntegerType(), True),
        StructField("views", IntegerType(), True),
        StructField("web_tags", StringType(), True),
        StructField("web_keywords", StringType(), True),
        StructField("auth_id", StringType(), True),
        StructField("auth_name", StringType(), True),
        StructField("auth_type", IntegerType(), True),
        StructField("source_id", StringType(), True),
        StructField("source_name", StringType(), True),
        StructField("reply_to", StringType(), True),
        StructField("level", IntegerType(), True),
        StructField("org_id", IntegerType(), True),
        StructField("sentiment", IntegerType(), True),
        StructField("source_url", StringType(), True),
        StructField("auth_url", StringType(), True),
        StructField("createdAt", StringType(), True),
        StructField("post_id", StringType(), True)
    ])

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
        , spark_conf=spark_conf
        , log_level="INFO"
    )

    spark_configs = get_spark_config()
    # print(spark_configs["postgres"])

    df = spark_connect.spark.read.format("mongo") \
        .option("uri", spark_configs["mongodb"]["uri"]) \
        .option("database", spark_configs["mongodb"]["database"]) \
        .option("collection", spark_configs["mongodb"]["collection_posts"]) \
        .schema(schema) \
        .load()

    # df = df.select(col("createdAt")).show(truncate= False)

    df = df.withColumn("createdAt", to_timestamp(col("createdAt"), "yyyy-MM-dd HH:mm:ss.SSSSSS"))
    # df = df.select(col("createdAt")).show(truncate= False)


    # Lấy thời gian hiện tại theo múi giờ Việt Nam (UTC+7)
    tz = pytz.timezone("Asia/Ho_Chi_Minh")
    current_time = datetime.now(tz)

    # 0h sáng hôm nay
    start_time = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S.000000")
    print(start_time_str)

    # 0h sáng ngày mai
    end_time = start_time + timedelta(days=1)
    end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S.000000")
    print(end_time_str)

    # Convert string times to timestamps for filtering
    # start_time_ts = to_timestamp(lit(start_time_str), "yyyy-MM-dd HH:mm:ss.SSSSSS")
    # end_time_ts = to_timestamp(lit(end_time_str), "yyyy-MM-dd HH:mm:ss.SSSSSS")

    # df = df.withColumn("createdAt", from_utc_timestamp(col("createdAt"), "Asia/Ho_Chi_Minh"))

    # Lọc dữ liệu
    df = df.filter((col("createdAt") >= start_time_str) & (col("createdAt") <= end_time_str))
    # df.show()

    df = df.withColumn("is_sent", lit(0)) \
        .withColumn("spam", lit(0)) \
        .withColumn("completed", lit(0)) \
        .withColumn("bot_id", lit("bot_cron")) \
        .select(
            col("post_id").alias("id")
            # col("id")
            , col("org_id")
            , col("doc_type")
            , col("source_type")
            , col("crawl_source")
            , col("crawl_source_code")
            , col("pub_time")
            , col("crawl_time")
            , col("subject_id")
            , col("title")
            , col("description")
            , col("content")
            , col("url")
            , col("media_urls")
            , col("comments")
            , col("shares")
            , col("reactions")
            , col("favors")
            , col("views")
            , col("web_tags")
            , col("web_keywords")
            , col("auth_id")
            , col("auth_name")
            , col("auth_type")
            , col("source_id")
            , col("source_name")
            , col("reply_to")
            , col("level")
            , col("sentiment")
            , col("is_sent")
            , col("spam")
            , col("completed")
            , col("source_url")
            , col("auth_url")
            , col("bot_id")
    ).dropDuplicates(["id"])
    df.show(truncate= False)
    # df.printSchema()
    # print(df.count())

    df_write = SparkWriteDatabases(spark_connect.spark, spark_configs)
    df_write.spark_validate_before_write_comment_postgres(df, spark_configs["postgres"]["config"]["table_posts"], spark_configs["postgres"],"append")

    spark_connect.spark.stop()


if __name__ == "__main__":
    main()