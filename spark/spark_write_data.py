from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from database.postgres_connect import PostgresConnect
from get_config.database_config import get_database_config


class SparkWriteDatabases:
    def __init__(self, spark : SparkSession, spark_config : Dict):
        self.spark = spark
        self.db_config = spark_config

    def spark_write_postgres(self, df : DataFrame, table_name : str, spark_config: Dict[str, str], mode : str = "append"):
        try:
            postgres_client = PostgresConnect(spark_config["config"]["host"], spark_config["config"]["port"], spark_config["config"]["user"], spark_config["config"]["password"], spark_config["config"]["database"])
            postgres_client.connect()
            postgres_client.close()
        except Exception as e:
            raise Exception(f"-------------Failed to connect Postgres: {e}---------------")

        df.write \
            .format("jdbc") \
            .option("url", spark_config["jdbc_url"]) \
            .option("dbtable", table_name) \
            .option("user", spark_config["config"]["user"]) \
            .option("password", spark_config["config"]["password"]) \
            .option("driver", spark_config["config"]["driver"]) \
            .mode(mode) \
            .save()

        print(f"Spark have written data to Postgres in table : {table_name}")

    def spark_validate_before_write_postgres(self, df_write: DataFrame, table_name: str, spark_config: Dict[str, str], mode: str = "append"):
        df_read = self.spark.read \
            .format("jdbc") \
            .option("url", spark_config["jdbc_url"]) \
            .option("dbtable", "{}.{}".format(spark_config["config"]["schema"], table_name)) \
            .option("user", spark_config["config"]["user"]) \
            .option("password", spark_config["config"]["password"]) \
            .option("driver", spark_config["config"]["driver"]) \
            .load()

        # df_read = df_read.select(col("product_id"),col("category_id"))

        def subtract_dataframe(df_spark_write: DataFrame, df_read_database: DataFrame):
            #subtract 2 datafame
            result = df_spark_write.subtract(df_read_database)
            result.show()
            print(f"-------------Result records: {result.count()}--------------------")
            if not result.isEmpty():
                result.write \
                    .format("jdbc") \
                    .option("url", spark_config["jdbc_url"]) \
                    .option("dbtable", "{}.{}".format(spark_config["config"]["schema"], table_name)) \
                    .option("user", spark_config["config"]["user"]) \
                    .option("password", spark_config["config"]["password"]) \
                    .option("driver", spark_config["config"]["driver"]) \
                    .mode(mode) \
                    .save()

        if df_read.count() == df_write.count():
            print(f"---------------------validate {df_read.count()} records success-----------------------")
            subtract_dataframe(df_write, df_read)
            print(f"---------------validate data of records success----------------")
        else:
            subtract_dataframe(df_write,df_read)
            print(f"-------------insert missing records by using spark insert data-----------------")

    def spark_validate_before_write_comment_postgres(self, df_write: DataFrame, table_name: str, spark_config: Dict[str, str], mode: str = "append"):
        df_read = self.spark.read \
            .format("jdbc") \
            .option("url", spark_config["jdbc_url"]) \
            .option("dbtable", table_name) \
            .option("user", spark_config["config"]["user"]) \
            .option("password", spark_config["config"]["password"]) \
            .option("driver", spark_config["config"]["driver"]) \
            .load()

        df_read = df_read.select(
            col("url")
            , col("org_id")
        )

        def subtract_dataframe(df_spark_write: DataFrame, df_read_database: DataFrame):
            #subtract 2 datafame
            result = df_spark_write.select(
                col("url")
                , col("org_id")
            ).subtract(df_read_database)
            # result.show()
            print(f"-------------Result records: {result.count()}--------------------")
            if not result.isEmpty():
                diff_records = df_spark_write.join(result,["url","org_id"], "inner")
                diff_records.show(truncate= False)
                diff_records.write \
                    .format("jdbc") \
                    .option("url", spark_config["jdbc_url"]) \
                    .option("dbtable", table_name) \
                    .option("user", spark_config["config"]["user"]) \
                    .option("password", spark_config["config"]["password"]) \
                    .option("driver", spark_config["config"]["driver"]) \
                    .mode(mode) \
                    .save()

        if df_read.count() == df_write.count():
            print(f"---------------------validate {df_read.count()} records success-----------------------")
            subtract_dataframe(df_write, df_read)
            print(f"---------------validate data of records success to Postgres----------------")
            config = get_database_config()
            try:
                with PostgresConnect(
                        config["postgres"].host,
                        config["postgres"].port,
                        config["postgres"].user,
                        config["postgres"].password,
                        config["postgres"].database
                ) as postgres_client:
                    connection, cursor = postgres_client.connection, postgres_client.cursor

                    # Execute the function
                    cursor.execute("SELECT insert_temp_to_post_v3(botid := %s)", ('bot_cron',))

                    # Fetch results if the function returns data
                    result = cursor.fetchall()
                    print(f"---------------------Số bản ghi sắp insert vào tbl_posts: {result}--------------------------")

                    cursor.execute("SELECT insert_analysis_posts_v2(ARRAY[%s])",
                                   ([2, 3, 6, 4, 7, 11, 12, 16, 17, 18, 19, 20, 21, 22, 25, 24, 23, 26, 8, 9],))
                    result1 = cursor.fetchall()
                    print(
                        f"---------------------Số bản ghi sắp insert vào analysis_posts: {result1}--------------------------")

                    # Commit the transaction
                    connection.commit()
                    print("--------------------------Thêm vào tools thành công.-----------------------")

            except Exception as e:
                print(f"Lỗi: {e}")
        else:
            subtract_dataframe(df_write,df_read)
            print(f"-------------insert missing records by using spark insert data to Postgres-----------------")
            config = get_database_config()
            try:
                with PostgresConnect(
                        config["postgres"].host,
                        config["postgres"].port,
                        config["postgres"].user,
                        config["postgres"].password,
                        config["postgres"].database
                ) as postgres_client:
                    connection, cursor = postgres_client.connection, postgres_client.cursor

                    # Execute the function
                    cursor.execute("SELECT insert_temp_to_post_v3(botid := %s)", ('bot_cron',))

                    # Fetch results if the function returns data
                    result = cursor.fetchall()
                    print(f"----------------------Số bản ghi sắp insert vào tbl_posts: {result}--------------------------")

                    cursor.execute("SELECT insert_analysis_posts_v2(ARRAY[%s])",
                                   ([2, 3, 6, 4, 7, 11, 12, 16, 17, 18, 19, 20, 21, 22, 25, 24, 23, 26, 8, 9],))
                    result1 = cursor.fetchall()
                    print(
                        f"---------------------Số bản ghi sắp insert vào analysis_posts: {result1}--------------------------")

                    # Commit the transaction
                    connection.commit()
                    print("----------------------Thêm vào tools thành công.-------------------------------")

            except Exception as e:
                print(f"Lỗi: {e}")

    def spark_validate_before_write_detail_postgres(self, df_write: DataFrame, table_name: str, postgres_config: Dict[str, str], clickhouse_config: Dict[str, str], mode: str = "append"):
        df_read = self.spark.read \
            .format("jdbc") \
            .option("url", postgres_config["jdbc_url"]) \
            .option("dbtable", "{}.{}".format(postgres_config["config"]["schema"], table_name)) \
            .option("user", postgres_config["config"]["user"]) \
            .option("password", postgres_config["config"]["password"]) \
            .option("driver", postgres_config["config"]["driver"]) \
            .load()

        # df_read = df_read.select(
        #     col("comment_id")
        #     , col("author_id")
        #     , col("variation_id")
        # )
        # df_read.show()
        def subtract_dataframe(df_spark_write: DataFrame, df_read_database: DataFrame):
            #subtract 2 datafame
            result = df_spark_write.subtract(df_read_database.drop("detail_surge_id"))
            result.show()
            print(f"-------------Result records: {result.count()}--------------------")
            if not result.isEmpty():
                # result.write \
                #     .format("jdbc") \
                #     .option("url", clickhouse_config["jdbc_url"]) \
                #     .option("dbtable", table_name) \
                #     .option("user", clickhouse_config["config"]["user"]) \
                #     .option("password", clickhouse_config["config"]["password"]) \
                #     .option("driver", clickhouse_config["config"]["driver"]) \
                #     .mode(mode) \
                #     .save()
                result.write \
                    .format("jdbc") \
                    .option("url", postgres_config["jdbc_url"]) \
                    .option("dbtable", "{}.{}".format(postgres_config["config"]["schema"], table_name)) \
                    .option("user", postgres_config["config"]["user"]) \
                    .option("password", postgres_config["config"]["password"]) \
                    .option("driver", postgres_config["config"]["driver"]) \
                    .mode(mode) \
                    .save()


        if df_read.count() == df_write.count():
            print(f"---------------------validate {df_read.count()} records success-----------------------")
            subtract_dataframe(df_write, df_read)
            print(f"---------------validate data of records success----------------")

        else:
            subtract_dataframe(df_write,df_read)
            print(f"-------------insert missing records by using spark insert data-----------------")

    def spark_write_mongodb(self, df: DataFrame, database: str, collection: str, uri: str, mode: str = "append"):
        df.write \
            .format("mongo") \
            .option("uri", uri) \
            .option("database", database) \
            .option("collection", collection) \
            .mode(mode) \
            .save()

        print(f"----------------Spark writed data to mongodb in collection: {database}.{collection}---------------")

    def spark_validate_before_write_mongodb(self, df_write: DataFrame, database : str, collection: str, uri: str, mode: str = "append"):
        df_read = self.spark.read \
            .format("mongo") \
            .option("uri", uri) \
            .option("database", database) \
            .option("collection", collection) \
            .load()

        df_read = df_read.select(
            col("post_id")
        )
        # df_read.show()
        def subtract_dataframe(df_spark_write: DataFrame, df_read_database: DataFrame):
            #subtract 2 datafame
            result = df_spark_write.select("post_id").subtract(df_read_database)
            # result.show()
            print(f"-------------Result records: {result.count()}--------------------")
            if not result.isEmpty():
                diff_records = df_spark_write.join(result, ["post_id"], "inner")
                diff_records.show(truncate= False)
                print(f"=========================={diff_records.count()}=========================================")
                diff_records.write \
                    .format("mongo") \
                    .option("uri", uri) \
                    .option("database", database) \
                    .option("collection", collection) \
                    .mode(mode) \
                    .save()


        if df_read.count() == df_write.count():
            print(f"---------------------validate {df_read.count()} records success-----------------------")
            subtract_dataframe(df_write, df_read)
            print(f"---------------validate data of records success to MongoDB----------------")

        else:
            subtract_dataframe(df_write,df_read)
            print(f"-------------insert missing records by using spark insert data to MongoDB-----------------")

    # def spark_write_clickhouse(self, df : DataFrame, table_name : str, spark_config: Dict[str, str], mode : str = "append"):
    #     try:
    #         clickhouse_client = ClickhouseConnect(spark_config["config"]["host"], spark_config["config"]["port"], spark_config["config"]["user"], spark_config["config"]["password"])
    #         clickhouse_client.connect()
    #         clickhouse_client.close()
    #     except Exception as e:
    #         raise Exception(f"----------Failed to connect Click House: {e}---------------")
    #     if df.isEmpty():
    #         print(f"-----------Spark writed none data to Click House in table: {table_name}------------------")
    #     else:
    #         df.write \
    #             .format("jdbc") \
    #             .option("url", spark_config["jdbc_url"]) \
    #             .option("dbtable", table_name) \
    #             .option("user", spark_config["config"]["user"]) \
    #             .option("password", spark_config["config"]["password"]) \
    #             .option("driver", spark_config["config"]["driver"]) \
    #             .mode(mode) \
    #             .save()
    #
    #     print(f"-----------Spark writed data to Click House in table: {table_name}------------------")
    #
    #     try:
    #         clickhouse_client = ClickhouseConnect(spark_config["config"]["host"], spark_config["config"]["port"],
    #                                               spark_config["config"]["user"], spark_config["config"]["password"])
    #         client = clickhouse_client.connect()
    #         client.execute(f"OPTIMIZE TABLE {spark_config["config"]["database"]}.{table_name} FINAL DEDUPLICATE;")
    #         clickhouse_client.close()
    #         print(f"------------OPTIMIZED TABLE {table_name}---------------------")
    #     except Exception as e:
    #         raise Exception(f"----------Failed to connect Click House: {e}---------------")
