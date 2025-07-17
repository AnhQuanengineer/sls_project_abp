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
from datetime import datetime, timedelta
from time import mktime


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
        StructField("createdAt", TimestampType(), True),
        StructField("post_id", StringType(), True)
    ])

    schema_keywords = StructType([
                StructField("code", StringType(), True),
                StructField("org_id", IntegerType(), True)
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
        # , spark_conf=spark_conf
        , log_level="INFO"
    )

    spark_configs = get_spark_config()
    # print(spark_configs["postgres"])

    df_posts = spark_connect.spark.read.format("mongo") \
        .option("uri", spark_configs["mongodb"]["uri"]) \
        .option("database", spark_configs["mongodb"]["database"]) \
        .option("collection", spark_configs["mongodb"]["collection_posts"]) \
        .schema(schema) \
        .load()

    df_posts = df_posts.withColumn("createdAt", expr("createdAt - interval 7 hours"))
    # df_posts.select(col("createdAt")).orderBy(col("createdAt").desc()).show(truncate=False)

    df_posts = df_posts.filter(
        (col("createdAt") >= current_date()) &
        (col("createdAt") < date_add(current_date(), 1))
    )
    # print(df_posts.count())

    # df_posts = df_posts.select(col("createdAt")).show(truncate= False)

    # df_posts = df_posts.withColumn("createdAt", to_timestamp(col("createdAt"), "yyyy-MM-dd HH:mm:ss.SSSSSS"))

    #==========================================================================
    # # Lấy thời gian hiện tại theo múi giờ Việt Nam (UTC+7)
    # tz = pytz.timezone("Asia/Ho_Chi_Minh")
    # current_time = datetime.now(tz)
    #
    # # 0h sáng hôm nay
    # start_time = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
    # start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S.000000")
    # print(start_time)
    #
    # # 0h sáng ngày mai
    # end_time = start_time + timedelta(days=1)
    # end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S.000000")
    # print(end_time)

    # Lọc dữ liệu
    # df_posts = df_posts.filter((col("createdAt") >= start_time) & (col("createdAt") <= end_time))


    # =========================================================================
    # current_time = datetime.now()
    # # 0h tomorrow
    # end_time = (current_time + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    # end_timestamp = int(mktime(end_time.timetuple()))
    # print(end_timestamp)
    # # 8h yesterday
    # start_time = (current_time - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    # # print(start_time)
    # start_timestamp = int(mktime(start_time.timetuple()))
    # print(start_timestamp)
    # # filter from date
    #
    # df_posts = df_posts.filter((col("pub_time") >= start_timestamp) & (col("pub_time") <= end_timestamp))

    #==============================================================
    # Convert string times to timestamps for filtering
    # start_time_ts = to_timestamp(lit(start_time_str), "yyyy-MM-dd HH:mm:ss.SSSSSS")
    # end_time_ts = to_timestamp(lit(end_time_str), "yyyy-MM-dd HH:mm:ss.SSSSSS")


    df_orgs = spark_connect.spark.read.format("mongo") \
        .option("uri", spark_configs["mongodb"]["uri"]) \
        .option("database", spark_configs["mongodb"]["database"]) \
        .option("collection", spark_configs["mongodb"]["collection_key_words"]) \
        .schema(schema_keywords) \
        .load()
    # df_orgs.show()

    df_postgres = df_posts.join(df_orgs,["org_id"],"inner")

    df_postgres = df_postgres.withColumn("is_sent", lit(0)) \
        .withColumn("spam", lit(0)) \
        .withColumn("completed", lit(0)) \
        .withColumn("bot_id", lit("bot_cron")) \
        .withColumn("unique_id", monotonically_increasing_id()) \
        .withColumn("uid", substring(md5(concat(col("unique_id"), expr("rand()"))), 1, 16)) \
        .withColumn("id", concat_ws("-", "code","crawl_source_code","source_type","auth_type","doc_type","uid")) \
        .select(
            # col("post_id").alias("id")
            col("id")
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
    ).dropDuplicates(["url","org_id"])
    # df_postgres.show(truncate= False)
    # df.printSchema()
    # print(df.count())

    df_write = SparkWriteDatabases(spark_connect.spark, spark_configs)
    df_write.spark_validate_before_write_comment_postgres(df_postgres, spark_configs["postgres"]["config"]["table_posts"], spark_configs["postgres"],"append")

    spark_connect.spark.stop()


if __name__ == "__main__":
    main()