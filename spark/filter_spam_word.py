from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from get_config.spark_config import SparkConnect, get_spark_config


def main():

    file_jar_package = [
            "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
            "org.postgresql:postgresql:42.7.4"
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
        StructField("spamwords", ArrayType(StringType()), True),
    ])

    schema_post = StructType([
        StructField("auth_id", StringType(), True),
        StructField("auth_name", StringType(), True),
        StructField("auth_type", IntegerType(), True),
        StructField("auth_url", StringType(), True),
        StructField("comments", IntegerType(), True),
        StructField("content", StringType(), True),
        StructField("crawl_source", IntegerType(), True),
        StructField("crawl_source_code", StringType(), True),
        StructField("crawl_time", LongType(), True),
        StructField("description", StringType(), True),
        StructField("doc_type", IntegerType(), True),
        StructField("favors", IntegerType(), True),
        StructField("level", StringType(), True),
        StructField("media_urls", StringType(), True),
        StructField("pg_sync", BooleanType(), True),
        StructField("post_id", StringType(), True),
        StructField("pub_time", LongType(), True),
        StructField("reactions", IntegerType(), True),
        StructField("reply_to", StringType(), True),
        StructField("sentiment", IntegerType(), True),
        StructField("shares", IntegerType(), True),
        StructField("source_id", StringType(), True),
        StructField("source_name", StringType(), True),
        StructField("source_type", IntegerType(), True),
        StructField("source_url", StringType(), True),
        StructField("subject_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("url", StringType(), True),
        StructField("views", IntegerType(), True),
        StructField("web_keywords", StringType(), True),
        StructField("web_tags", StringType(), True)
    ])

    # Đọc dữ liệu
    # posts_df = spark_connect.spark.read.schema(schema_post).json("/home/victo/data_grid/data_grid_dowload/sls_not_spam_posts.json")
    # posts_df.show()
    # orgs_df = spark_connect.spark.read.schema(schema_keywords).json("/home/victo/data_grid/data_grid_dowload/sls_etl_orgs.json")
    # orgs_df.show()

    orgs_df = spark_connect.spark.read.format("mongo") \
        .option("uri", spark_configs["mongodb"]["uri"]) \
        .option("database", spark_configs["mongodb"]["database"]) \
        .option("collection", spark_configs["mongodb"]["collection_product"]) \
        .schema(schema_keywords) \
        .load()

    # orgs_df = orgs_df.filter(col("org_id") == 6)

    # orgs_df.show()

    posts_df = spark_connect.spark.read.format("mongo") \
        .option("uri", spark_configs["mongodb"]["uri"]) \
        .option("database", spark_configs["mongodb"]["database"]) \
        .option("collection", spark_configs["mongodb"]["collection_shop"]) \
        .option("partitioner", "MongoShardedPartitioner") \
        .option("partitionerOptions.numPartitions", "2") \
        .schema(schema_post) \
        .load()

    # print(posts_df.rdd.getNumPartitions())
#
    posts_df = posts_df.limit(100)
    # print(posts_df.count())

    # posts_df.show()

    # Chuyển content thành chữ thường
    posts_df = posts_df.withColumn("content_lower", lower(col("content")))

    # UDF kiểm tra spamwords
    def contains_spamwords(content, spamwords):
        if content and isinstance(content, str):
            if not spamwords:
                return False
            # content_lower = content.lower()
            return any(spamword.lower() in content for spamword in spamwords)
        return None


    contains_spamwords_udf = udf(contains_spamwords, BooleanType())

    # Thu thập tất cả spamwords
    all_spamwords = orgs_df.select(explode(col("spamwords")).alias("spamword")).distinct().collect()
    all_spamwords_list = [row["spamword"] for row in all_spamwords]

    # Lọc bài viết chứa spamwords
    posts_df = posts_df.withColumn("has_spamwords", contains_spamwords_udf(col("content_lower"), udf(lambda: all_spamwords_list, ArrayType(StringType()))()))
    posts_filtered_df = posts_df.filter(~col("has_spamwords"))


    # Bước 2: Tìm từ khóa khớp trong content
    # Thu thập tất cả keywords từ orgs_df
    all_keywords = orgs_df.select(explode(col("keywords")).alias("keyword")).distinct().collect()
    all_keywords_list = [row["keyword"] for row in all_keywords]

    # UDF để tìm các từ khóa xuất hiện trong content
    def find_matched_keywords(content, keywords):
        # content_lower = content.lower()
        return [keyword for keyword in keywords if keyword.lower() in content]

    find_matched_keywords_udf = udf(find_matched_keywords, ArrayType(StringType()))

    # Thêm cột matched_keywords vào posts_filtered_df
    posts_filtered_df = posts_filtered_df.withColumn(
        "matched_keywords",
        find_matched_keywords_udf(col("content_lower"), udf(lambda: all_keywords_list, ArrayType(StringType()))())
    )
    # print(posts_filtered_df.count())


    # Bước 3: Chuẩn bị orgs_df cho inner join
    # Explode keywords_array trong orgs_df để tạo cột keyword
    orgs_exploded_df = orgs_df.select("org_id", explode(col("keywords")).alias("keyword"))

    # Bước 4: Sử dụng inner join để gán org_id
    # Explode matched_keywords trong posts_filtered_df
    posts_exploded_df = posts_filtered_df.select(
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
        , col("pg_sync")
        , col("post_id")
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
        # , col("matched_keywords")
        , explode_outer(col("matched_keywords")).alias("keyword")
    )
    # print(posts_exploded_df.count())
    # posts_exploded_df.show()

    # Inner join với orgs_exploded_df
    posts_joined_df = posts_exploded_df.join(
        orgs_exploded_df,
        posts_exploded_df.keyword == orgs_exploded_df.keyword,
        "left_outer"
    )
    # posts_joined_df.show()
    # print(posts_joined_df.count())

    # Bước 5: Gộp các org_id cho mỗi bài viết
    # group_columns = [col(c) for c in posts_df.columns if c not in ["content_lower", "has_spamwords"]]
    # posts_classified_df = posts_joined_df.groupBy("post_id", "content").agg(
    #     collect_set("org_id").alias("org_ids"),
    #     first("auth_id").alias("auth_id"),
    #     first("auth_name").alias("auth_name"),
    #     first("auth_type").alias("auth_type"),
    #     first("auth_url").alias("auth_url"),
    #     first("comments").alias("comments"),
    #     first("crawl_source").alias("crawl_source"),
    #     first("crawl_source_code").alias("crawl_source_code"),
    #     first("crawl_time").alias("crawl_time"),
    #     first("description").alias("description"),
    #     first("doc_type").alias("doc_type"),
    #     first("favors").alias("favors"),
    #     first("level").alias("level"),
    #     first("media_urls").alias("media_urls"),
    #     first("pg_sync").alias("pg_sync"),
    #     first("pub_time").alias("pub_time"),
    #     first("reactions").alias("reactions"),
    #     first("reply_to").alias("reply_to"),
    #     first("sentiment").alias("sentiment"),
    #     first("shares").alias("shares"),
    #     first("source_id").alias("source_id"),
    #     first("source_name").alias("source_name"),
    #     first("source_type").alias("source_type"),
    #     first("source_url").alias("source_url"),
    #     first("subject_id").alias("subject_id"),
    #     first("title").alias("title"),
    #     first("url").alias("url"),
    #     first("views").alias("views"),
    #     first("web_keywords").alias("web_keywords"),
    #     first("web_tags").alias("web_tags")
    # )

    # posts_classified_df.show()

    # Làm phẳng matched_org_ids
    posts_classified_df = posts_joined_df.select(
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
        , col("org_id")
        , col("pg_sync")
        , col("post_id")
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
    )
    print(posts_classified_df.count())
    # posts_classified_df.show()
    #
    #
    # Bước 6: Lưu kết quả
    # posts_classified_df.write.mode("overwrite").json("/home/victo/PycharmProjects/sls_project/sql/output_classified_posts.json")

    # Dừng Spark Session
    spark_connect.spark.stop()
#
if __name__ == "__main__":
    main()