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


    schema = StructType([
        StructField("doc_type", LongType(), True),
        StructField("source_type", LongType(), True),
        StructField("crawl_source", LongType(), True),
        StructField("crawl_source_code", StringType(), True),
        StructField("pub_time", LongType(), True),
        StructField("crawl_time", LongType(), True),
        StructField("sentiment", LongType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("content", StringType(), True),
        StructField("url", StringType(), True),
        StructField("media_urls", StringType(), True),
        StructField("subject_id", StringType(), True),
        StructField("web_tags", StringType(), True),
        StructField("web_keywords", StringType(), True),
        StructField("reactions", LongType(), True),
        StructField("comments", LongType(), True),
        StructField("shares", LongType(), True),
        StructField("favors", LongType(), True),
        StructField("views", LongType(), True),
        StructField("auth_id", StringType(), True),
        StructField("auth_url", StringType(), True),
        StructField("auth_name", StringType(), True),
        StructField("auth_type", LongType(), True),
        StructField("source_id", StringType(), True),
        StructField("source_url", StringType(), True),
        StructField("source_name", StringType(), True),
        StructField("reply_to", StringType(), True),
        StructField("level", LongType(), True),
        StructField("id", StringType(), True),
        StructField("mg_sync", BooleanType(), True)
    ])

    # read post from els
    posts_df = spark_connect.spark.read.format("org.elasticsearch.spark.sql") \
        .option("es.nodes", spark_configs["elastic_search"]["host"]) \
        .option("es.port", spark_configs["elastic_search"]["port"]) \
        .option("es.resource", spark_configs["elastic_search"]["index_not_classify"]) \
        .option("es.read.metadata", "true") \
        .option("es.net.http.auth.user", spark_configs["elastic_search"]["user"]) \
        .option("es.net.http.auth.pass", spark_configs["elastic_search"]["password"]) \
        .option("es.net.ssl", "false") \
        .option("es.nodes.wan.only", "true") \
        .schema(schema) \
        .load()

    # posts_df = spark_connect.spark.read.format("mongo") \
    #     .option("uri", spark_configs["mongodb"]["uri"]) \
    #     .option("database", spark_configs["mongodb"]["database"]) \
    #     .option("collection", "data_unclassified") \
    #     .schema(schema) \
    #     .load()

    # posts_df.show()
    # print(posts_df.count())

    schema_keywords = StructType([
        StructField("code", StringType(), True),
        StructField("keywords", ArrayType(StringType()), True),
        StructField("org_id", IntegerType(), True),
        StructField("spamwords", ArrayType(StringType()), True),
    ])

    #read key word
    orgs_df = spark_connect.spark.read.format("mongo") \
        .option("uri", spark_configs["mongodb"]["uri"]) \
        .option("database", spark_configs["mongodb"]["database"]) \
        .option("collection", spark_configs["mongodb"]["collection_key_words"]) \
        .schema(schema_keywords) \
        .load()

    # orgs_df.show()

    json_schema = StructType([
        StructField("match_phrase", StructType([
            StructField("content", StringType())
        ]))
    ])

    orgs_df = orgs_df.withColumn(
        "spamwords",
        transform(col("spamwords"), lambda x: from_json(x, json_schema).match_phrase.content)
    )

    # orgs_df.select("spamwords").show(truncate=False)

    current_time = datetime.now()
    # 0h tomorrow
    end_time = (current_time + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_timestamp = int(mktime(end_time.timetuple()))
    # print(end_time)
    # 8h yesterday
    start_time = (current_time - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
    # print(start_time)
    start_timestamp = int(mktime(start_time.timetuple()))
    # print(start_timestamp)
    #filter from date

    posts_df = posts_df.withColumn("level", col("level").cast(IntegerType()))
    posts_df = posts_df.filter((col("pub_time") >= start_timestamp) & (col("pub_time") <= end_timestamp))
    posts_df = posts_df.filter((col("content").isNotNull()) | (col("content") != ""))

    # print(posts_df.count())

    # change to lower case
    posts_df = posts_df.withColumn("content_lower", lower(col("content")))
    # posts_df.printSchema()

    # check spam word
    def contains_spamwords(content, spamwords):
        if content and isinstance(content, str):
            if not spamwords:
                return False
            content_lower = content.lower()
            return any(spamword.lower() in content_lower for spamword in spamwords)
        return None

    contains_spamwords_udf = udf(contains_spamwords, BooleanType())

    # collect spam word
    all_spamwords = orgs_df.select(explode(col("spamwords")).alias("spamword")).distinct().collect()
    all_spamwords_list = [row["spamword"] for row in all_spamwords]
    # print(all_spamwords_list)

    # filter spam post
    posts_df = posts_df.withColumn("has_spamwords", contains_spamwords_udf(col("content_lower"),
                                                                           udf(lambda: all_spamwords_list,
                                                                               ArrayType(StringType()))()))
    # posts_df.show()
    posts_filtered_df = posts_df.filter(~col("has_spamwords"))
    # posts_filtered_df.where(col("has_spamwords") == True).show()


    # print(posts_filtered_df.count()
    # posts_filtered_df.show()

    # find key word in content
    # colect key word
    all_keywords = orgs_df.select(explode(col("keywords")).alias("keyword")).distinct().collect()
    all_keywords_list = [row["keyword"] for row in all_keywords]
    # print(all_keywords_list)

    # find kwy word in content
    def find_matched_keywords(content, keywords):
        content_lower = content.lower()
        return [keyword for keyword in keywords if keyword.lower() in content_lower]

    find_matched_keywords_udf = udf(find_matched_keywords, ArrayType(StringType()))

    posts_filtered_df = posts_filtered_df.withColumn(
        "matched_keywords",
        find_matched_keywords_udf(col("content_lower"), udf(lambda: all_keywords_list, ArrayType(StringType()))())
    )
    # print(posts_filtered_df.count())

    # classify organization
    orgs_exploded_df = orgs_df.select("org_id", explode(col("keywords")).alias("keyword"))
    # orgs_exploded_df.show()

    posts_exploded_df = posts_filtered_df \
        .select(
        col("auth_id")
        , col("auth_name")
        , col("auth_type")
        , col("auth_url")
        , col("comments")
        , col('content')
        , col('crawl_source')
        , col("crawl_source_code")
        , col("crawl_time")
        , col("description")
        , col("doc_type")
        , col("favors")
        , col("level")
        , col("media_urls")
        , col("pub_time")
        , col("reactions")
        , col("reply_to")
        , col("sentiment")
        , col("shares")
        , col("source_id")
        , col("source_name")
        , col("source_type")
        , col("source_url")
        , col("subject_id")
        , col("title")
        , col("url")
        , col("views")
        , col("web_keywords")
        , col("web_tags")
        # , col("matched_keywords").alias("keyword")
        , explode(col("matched_keywords")).alias("keyword")
    )
    # print(posts_exploded_df.count())
    # posts_exploded_df.show(truncate= False)

    # Inner join với orgs_exploded_df
    posts_joined_df = posts_exploded_df.join(
        orgs_exploded_df,
        posts_exploded_df.keyword == orgs_exploded_df.keyword,
        "inner"
    )

    posts_classified_df = posts_joined_df.withColumn("post_id", concat_ws("-", "url", "org_id")) \
        .withColumn("createdAt", expr("current_timestamp() + interval 7 hours")) \
        .withColumn("updatedAt", expr("current_timestamp() + interval 7 hours")) \
        .withColumn("pg_sync", lit(False)) \
        .select(
        col("post_id").cast(StringType())
        , col("doc_type").cast(IntegerType())
        , col("source_type").cast(IntegerType())
        , col('crawl_source').cast(IntegerType())
        , col("crawl_source_code").cast(StringType())
        , col("pub_time").cast(LongType())
        , col("crawl_time").cast(LongType())
        , col("subject_id").cast(StringType())
        , col("title").cast(StringType())
        , col("description").cast(StringType())
        , col("content").cast(StringType())
        , col("url").cast(StringType())
        , col("media_urls").cast(StringType())
        , col("comments").cast(IntegerType())
        , col("shares").cast(IntegerType())
        , col("reactions").cast(IntegerType())
        , col("favors").cast(IntegerType())
        , col("views").cast(IntegerType())
        , col("web_tags").cast(StringType())
        , col("web_keywords").cast(StringType())
        , col("auth_id").cast(StringType())
        , col("auth_name").cast(StringType())
        , col("auth_type").cast(IntegerType())
        , col("auth_url").cast(StringType())
        , col("source_id").cast(StringType())
        , col("source_name").cast(StringType())
        , col("source_url").cast(StringType())
        , col("reply_to").cast(StringType())
        , col("level")
        , col("org_id").cast(IntegerType())
        , col("sentiment").cast(IntegerType())
        , col("pg_sync").cast(BooleanType())
        , col("createdAt")
        , col("updatedAt")
    ).dropDuplicates(["post_id"])
    # print(posts_classified_df.count())
    # posts_classified_df.show(truncate= False)
    # posts_classified_df.printSchema()
    df_write = SparkWriteDatabases(spark_connect.spark, spark_configs)
    df_write.spark_validate_before_write_mongodb(posts_classified_df,spark_configs["mongodb"]["database"], spark_configs["mongodb"]["collection_posts"], spark_configs["mongodb"]["uri"], "append")

    # posts_classified_df.show(truncate= False)
    spark_connect.spark.stop()

if __name__ == "__main__":
    main()