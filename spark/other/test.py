from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQuery
import json
from get_config.spark_config import SparkConnect, get_spark_config


def main():

    file_jar_package = [
        "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
        "org.postgresql:postgresql:42.7.4",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    ]

    # Khởi tạo Spark Session
    spark_connect = SparkConnect(
        app_name="apb_streaming",
        master_url="local[*]",
        executor_memory="8g",
        executor_cores=4,
        driver_memory="4g",
        num_executors=1,
        jars=None,
        jar_packages=file_jar_package,
        spark_conf=None,
        log_level="INFO"
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

    # Đọc orgs_df từ MongoDB (dữ liệu tĩnh)
    orgs_df = spark_connect.spark.read.format("mongo") \
        .option("uri", spark_configs["mongodb"]["uri"]) \
        .option("database", spark_configs["mongodb"]["database"]) \
        .option("collection", spark_configs["mongodb"]["collection_product"]) \
        .schema(schema_keywords) \
        .load()

    # Broadcast orgs_df để tối ưu hóa join
    orgs_df = broadcast(orgs_df.cache())

    # Đọc dữ liệu streaming từ Kafka
    posts_df = spark_connect.spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sls_streaming") \
        .load()

    # Parse dữ liệu JSON từ Kafka
    posts_df = posts_df.select(
        from_json(col("value").cast("string"), schema_post).alias("data")
    ).select("data.*")

    # Giới hạn xử lý để kiểm tra (tùy chọn, có thể bỏ khi triển khai thực tế)
    # posts_df = posts_df.limit(10000)

    # Chuyển content thành chữ thường
    posts_df = posts_df.withColumn("content_lower", lower(col("content")))

    # UDF kiểm tra spamwords
    def contains_spamwords(content, spamwords):
        if content and isinstance(content, str):
            if not spamwords:
                return False
            return any(spamword.lower() in content for spamword in spamwords)
        return None

    contains_spamwords_udf = udf(contains_spamwords, BooleanType())

    # Thu thập tất cả spamwords
    all_spamwords = orgs_df.select(explode(col("spamwords")).alias("spamword")).distinct().collect()
    all_spamwords_list = [row["spamword"] for row in all_spamwords]

    # Lọc bài viết chứa spamwords
    posts_df = posts_df.withColumn(
        "has_spamwords",
        contains_spamwords_udf(col("content_lower"), udf(lambda: all_spamwords_list, ArrayType(StringType()))())
    )
    posts_filtered_df = posts_df.filter(~col("has_spamwords"))

    # Tìm từ khóa khớp trong content
    all_keywords = orgs_df.select(explode(col("keywords")).alias("keyword")).distinct().collect()
    all_keywords_list = [row["keyword"] for row in all_keywords]

    def find_matched_keywords(content, keywords):
        return [keyword for keyword in keywords if keyword.lower() in content]

    find_matched_keywords_udf = udf(find_matched_keywords, ArrayType(StringType()))

    posts_filtered_df = posts_filtered_df.withColumn(
        "matched_keywords",
        find_matched_keywords_udf(col("content_lower"), udf(lambda: all_keywords_list, ArrayType(StringType()))())
    )

    # Chuẩn bị orgs_df cho inner join
    orgs_exploded_df = orgs_df.select("org_id", explode(col("keywords")).alias("keyword"))

    # Explode matched_keywords trong posts_filtered_df
    posts_exploded_df = posts_filtered_df.select(
        col("auth_id"),
        col("auth_name"),
        col("auth_type"),
        col("auth_url"),
        col("comments"),
        col("content"),
        col("crawl_source"),
        col("crawl_source_code"),
        col("crawl_time"),
        col("description"),
        col("doc_type"),
        col("favors"),
        col("level"),
        col("media_urls"),
        col("pg_sync"),
        col("post_id"),
        col("pub_time"),
        col("reactions"),
        col("reply_to"),
        col("sentiment"),
        col("shares"),
        col("source_id"),
        col("source_name"),
        col("source_type"),
        col("source_url"),
        col("subject_id"),
        col("title"),
        col("url"),
        col("views"),
        col("web_keywords"),
        col("web_tags"),
        explode_outer(col("matched_keywords")).alias("keyword")
    )

    # Inner join với orgs_exploded_df
    posts_joined_df = posts_exploded_df.join(
        broadcast(orgs_exploded_df),
        posts_exploded_df.keyword == orgs_exploded_df.keyword,
        "inner"
    )
    # Làm phẳng org_ids
    posts_classified_df = posts_joined_df.select(
        col("auth_id"),
        col("auth_name"),
        col("auth_type"),
        col("auth_url"),
        col("comments"),
        col("content"),
        col("crawl_source"),
        col("crawl_source_code"),
        col("crawl_time"),
        col("description"),
        col("doc_type"),
        col("favors"),
        col("level"),
        col("media_urls"),
        col("org_id"),
        col("pg_sync"),
        col("post_id"),
        col("pub_time"),
        col("reactions"),
        col("reply_to"),
        col("sentiment"),
        col("shares"),
        col("source_id"),
        col("source_name"),
        col("source_type"),
        col("source_url"),
        col("subject_id"),
        col("title"),
        col("url"),
        col("views"),
        col("web_keywords"),
        col("web_tags")
    )

    # Ghi kết quả streaming vào MongoDB
    # query = posts_classified_df.writeStream \
    #     .format("mongo") \
    #     .option("uri", spark_configs["mongodb"]["uri"]) \
    #     .option("database", spark_configs["mongodb"]["database"]) \
    #     .option("collection", "classified_posts") \
    #     .option("checkpointLocation", "/tmp/checkpoints") \
    #     .outputMode("append") \
    #     .start()

    query = posts_classified_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()

    # Chờ query streaming hoàn thành
    query.awaitTermination()

if __name__ == "__main__":
    main()