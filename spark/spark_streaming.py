from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from get_config.spark_config import get_spark_config
from spark.spark_write_data import SparkWriteDatabases

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ShopeeProductProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3,org.mongodb.spark:mongo-spark-connector_2.12:10.5.0") \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("product_id", LongType(), True),
    StructField("property_name", StringType(), True),
    StructField("property_stock", IntegerType(), True),
    StructField("property_sold", IntegerType(), True),
    StructField("property_promotion_id", LongType(), True),
    StructField("property_price", IntegerType(), True),
    StructField("property_price_before_discount", IntegerType(), True),
    StructField("property_id", LongType(), True),
    StructField("shop_id", LongType(), True),
    StructField("crawl_time", TimestampType(), True),
    StructField("rating_star", DoubleType(), True)
])


# def split_model(model_str):
#     if model_str and isinstance(model_str, str):
#         if len(model_str.split(",")) == 2:
#             model1, model2 = model_str.split(",")
#             return {"model1": model1, "model2": model2}
#         else:
#             return {"model1": model_str, "model2": None}
#     return {"model1": None, "model2": None}

def split_model(model_str):
    if model_str and isinstance(model_str, str):
        result = []
        for i, value in enumerate(model_str.split(","), 1):
            name = f"name{i}"
            result.append({"name": name, "value": value.strip()})
        return result
    else:
        return []


# split_model_udf = udf(split_model,
#                       StructType([
#                           StructField("model1", StringType(), True),
#                           StructField("model2", StringType(), True)
#                       ]))

split_model_udf = udf(split_model, ArrayType(StructType([
        StructField("name", StringType(), True),
        StructField("value", StringType(), True)
    ])))

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.1.28:9092") \
    .option("subscribe", "shopee_product_details") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON
df_property = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

spark_configs = get_spark_config()


# Write to PostgreSQL
def write_to_postgres(df,epoch_id):
    spark_configs = get_spark_config()
    spark_config = spark_configs["postgres"]
    df.write \
        .format("jdbc") \
        .option("url", spark_config["jdbc_url"]) \
        .option("dbtable", "{}.Products_property".format(spark_config["config"]["schema"])) \
        .option("user", spark_config["config"]["user"]) \
        .option("password", spark_config["config"]["password"]) \
        .option("driver", spark_config["config"]["driver"]) \
        .mode("append") \
        .save()
#
# def write_to_mongodb(df,epoch_id):
#     spark_configs = get_spark_config()
#     spark_config = spark_configs["mongodb"]
#     df.write \
#         .format("mongo") \
#         .option("uri", spark_config["uri"]) \
#         .option("database", spark_config["database"]) \
#         .option("collection", "Streaming_kafka") \
#         .load()

df_property = df_property.withColumn("property_name", split_model_udf("property_name")) \
    .withColumn("name", concat_ws(",", "property_name.name")) \
    .withColumn("value", concat_ws(", ", "property_name.value")) \
    .select(
    col("property_id"),
    col("product_id"),
    col("name"),
    col("value"),
    col("property_price"),
    col("property_price_before_discount"),
    col("property_stock"),
    col("property_sold"),
    col("property_promotion_id"),
    col("rating_star"),
    col("crawl_time")
)

# query = df_property.writeStream \
#     .outputMode("append") \
#     .foreachBatch(write_to_postgres) \
#     .start()

# query = df_property.writeStream \
#     .format("mongodb") \
#     .option("uri", "mongodb://anhquan:123456@localhost:27017/shopee_data.CustomCollection?authSource=shopee_data") \
#     .option("database", "shopee_data") \
#     .option("collection", "Streaming_kafka") \
#     .option("checkpointLocation", "/tmp/checkpointDir") \
#     .option("forceDeleteTempCheckpointLocation", "true") \
#     .outputMode("append") \
#     .start()

# query = df_property.writeStream \
#   .format("mongodb") \
#   .option("checkpointLocation", "/tmp/pyspark/") \
#   .option("forceDeleteTempCheckpointLocation", "true") \
#   .option("spark.mongodb.connection.uri", "mongodb://anhquan:123456@localhost:27017") \
#   .option("spark.mongodb.database", "shopee_data") \
#   .option("spark.mongodb.collection", "Streaming_kafka") \
#   .outputMode("append") \
#   .start()

query = df_property \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", "/tmp/pyspark/") \
    .option("forceDeleteTempCheckpointLocation", "true") \
    .start()

query.awaitTermination()


# # Initialize Spark session (unchanged)
# spark = SparkSession.builder \
#     .appName("ShopeeProductProcessing") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3") \
#     .getOrCreate()
#
# # Schema and UDF (unchanged)
# schema = StructType([
#     StructField("product_id", LongType(), True),
#     StructField("property_name", StringType(), True),
#     StructField("property_stock", LongType(), True),
#     StructField("property_sold", LongType(), True),
#     StructField("property_promotion_id", LongType(), True),
#     StructField("property_price", LongType(), True),
#     StructField("property_price_before_discount", LongType(), True),
#     StructField("property_id", LongType(), True),
#     StructField("shop_id", LongType(), True),
#     StructField("crawl_time", TimestampType(), True),
#     StructField("rating_star", DoubleType(), True)
# ])
#
# def split_model(model_str):
#     if model_str and isinstance(model_str, str):
#         if len(model_str.split(",")) == 2:
#             model1, model2 = model_str.split(",")
#             return {"model1": model1, "model2": model2}
#         else:
#             return {"model1": model_str, "model2": None}
#     return {"model1": None, "model2": None}
#
# split_model_udf = udf(split_model, StructType([
#     StructField("model1", StringType(), True),
#     StructField("model2", StringType(), True)
# ]))
#
# # Read from Kafka
# kafka_df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "shopee-product") \
#     .option("startingOffsets", "earliest") \
#     .load()
#
# # Parse JSON
# df_property = kafka_df.selectExpr("CAST(value AS STRING)") \
#     .select(from_json(col("value"), schema).alias("data")) \
#     .select("data.*")
#
# # Add watermark and columns
# df_property = df_property.withWatermark("crawl_time", "10 minutes") \
#     .withColumn("model_name", split_model_udf("property_name")) \
#     .withColumn("minute", date_format(col("crawl_time"), "yyyy-MM-dd HH:mm"))
#
# # Aggregate using time-based window
# df_property_gmv = df_property.groupBy(
#     window(col("crawl_time"), "1 minute").alias("time_window"),
#     col("product_id"),
#     col("property_id")
# ).agg(
#     last("property_sold").alias("total_sales"),
#     last("property_price").alias("price")
# )
#
# # Create previous minute's data, preserving watermark
# df_property_gmv_prev = df_property_gmv.withColumnRenamed("total_sales", "prev_total_sales") \
#     .withColumn("window_start_prev", col("time_window.start"))
#
# # Keep timestamp for join and derive minute
# df_property_gmv = df_property_gmv.withColumn("window_start", col("time_window.start")) \
#     .withColumn("minute", date_format(col("time_window.start"), "yyyy-MM-dd HH:mm"))
#
# # Join with previous minute's data using time-range condition
# df_property_gmv = df_property_gmv.join(
#     df_property_gmv_prev.select(
#         col("window_start_prev"),
#         col("product_id").alias("product_id_prev"),
#         col("property_id").alias("property_id_prev"),
#         col("prev_total_sales")
#     ),
#     (df_property_gmv.product_id == col("product_id_prev")) &
#     (df_property_gmv.property_id == col("property_id_prev")) &
#     (df_property_gmv.window_start == col("window_start_prev") + expr("INTERVAL 1 MINUTE")),
#     "left"
# )
#
# # Calculate quantity and gmv
# df_property_gmv = df_property_gmv.withColumn(
#     "quantity",
#     when(col("prev_total_sales").isNotNull(), col("total_sales") - col("prev_total_sales")).otherwise(0)
# ).withColumn("gmv", col("quantity") * col("price"))
#
# # Select relevant columns
# df_property_gmv = df_property_gmv.select(
#     col("minute"),
#     col("product_id"),
#     col("property_id"),
#     col("quantity"),
#     col("gmv")
# )
#
# # Verify schemas before join
# df_property.printSchema()
# df_property_gmv.printSchema()
#
# # Join with original DataFrame
# df_property = df_property.join(
#     df_property_gmv,
#     ["minute", "product_id", "property_id"],
#     "left"
# ).fillna({"quantity": 0, "gmv": 0})
#
# # Select final columns
# df_property = df_property.select(
#     col("property_id"),
#     col("product_id"),
#     col("model_name.model1").alias("property_name_1"),
#     col("model_name.model2").alias("property_name_2"),
#     col("property_price"),
#     col("property_price_before_discount"),
#     col("property_stock"),
#     col("property_sold"),
#     col("property_promotion_id"),
#     col("quantity"),
#     col("gmv"),
#     col("rating_star"),
#     col("crawl_time")
# )
#
# # Write to console
# query = df_property.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("checkpointLocation", "/tmp/checkpoint") \
#     .start()
#
# query.awaitTermination()