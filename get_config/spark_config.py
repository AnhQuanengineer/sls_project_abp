import os.path
from typing import Optional, List, Dict

from pyspark.sql import SparkSession

from get_config.database_config import get_database_config


class SparkConnect:
    def __init__(
            self
            ,app_name: str
            ,master_url: str
            ,executor_memory: Optional[str] = '4g'
            ,executor_cores: Optional[int] = 2
            ,driver_memory: Optional[str] = "2g"
            ,num_executors: Optional[int] = 3
            ,jars: Optional[List[str]] = None
            ,jar_packages: Optional[List[str]] = None
            ,spark_conf: Optional[Dict[str,str]] = None
            ,log_level: str = "INFO"
    ):
        self.app_name = app_name
        self.spark = self.create_spark_session(master_url, executor_memory, executor_cores, driver_memory, num_executors, jars, jar_packages, spark_conf, log_level)

    def create_spark_session(
            self
            , master_url: str = "local[*]"
            , executor_memory: Optional[str] = '4g'
            , executor_cores: Optional[int] = 2
            , driver_memory: Optional[str] = "2g"
            , num_executors: Optional[int] = 3
            , jars: Optional[List[str]] = None
            , jar_packages: Optional[List[str]] = None
            , spark_conf: Optional[Dict[str, str]] = None
            , log_level: str = "INFO"
    ) -> SparkSession:
        builder = SparkSession.builder \
            .appName(self.app_name) \
            .master(master_url)

        if executor_memory:
            builder.config("spark.executor.memory", executor_memory)
        if executor_cores:
            builder.config("spark.executor.cores", executor_cores)
        if driver_memory:
            builder.config("spark.driver.memory", driver_memory)
        if num_executors:
            builder.config("spark.executor.instances", num_executors)

        if jars:
            jars_path = ",".join([os.path.abspath(jar) for jar in jars])
            builder.config("spark.jars", jars_path)

        if jar_packages:
            jar_packages_url = ",".join([jar_package for jar_package in jar_packages])
            builder.config("spark.jars.packages", jar_packages_url)

        # builder.config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        # builder.config("spark.sql.adaptive.enabled", "true")
        # builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # builder.config("spark.sql.session.timeZone", "UTC")

        if spark_conf:
            for key, value in spark_conf.items():
                builder.config(key, value)

        # builder.config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(log_level)

        return spark

    def stop(self):
        if self.spark:
            self.spark.stop()
            print("------------------Stop spark session----------------")

def get_spark_config() -> Dict:

    db_configs = get_database_config()

    return {
        "postgres" : {
            "jdbc_url" : "jdbc:postgresql://{}:{}/{}".format(db_configs["postgres"].host, db_configs["postgres"].port, db_configs["postgres"].database),
            "config" : {
                "host": db_configs["postgres"].host,
                "port": db_configs["postgres"].port,
                "user": db_configs["postgres"].user,
                "password": db_configs["postgres"].password,
                "database": db_configs["postgres"].database,
                "schema": db_configs["postgres"].schema,
                "driver": db_configs["postgres"].driver,
                "table_posts": db_configs["postgres"].table_posts
            }
        },
        "mongodb": {
            "database": db_configs["mongodb"].db_name,
            "collection_key_words": db_configs["mongodb"].collection_name_key_words,
            "collection_posts": db_configs["mongodb"].collection_name_posts,
            "uri": db_configs["mongodb"].uri
        },
        "elastic_search": {
            "host": db_configs["elastic_search"].host,
            "port": db_configs["elastic_search"].port,
            "user": db_configs["elastic_search"].user,
            "password": db_configs["elastic_search"].password,
            "index": db_configs["elastic_search"].index,
        }
        # "clickhouse": {
        #     "jdbc_url": "jdbc:clickhouse://{}:{}/{}".format(db_configs["clickhouse"].host, db_configs["clickhouse"].port_spark,
        #                                                     db_configs["clickhouse"].database),
        #     "config": {
        #         "host": db_configs["clickhouse"].host,
        #         "port": db_configs["clickhouse"].port,
        #         "user": db_configs["clickhouse"].user,
        #         "password": db_configs["clickhouse"].password,
        #         "database": db_configs["clickhouse"].database,
        #         "port_spark": db_configs["clickhouse"].port_spark,
        #         "driver": db_configs["clickhouse"].driver
        #     }
        # }
    }

if __name__ == "__main__":
    spark_config = get_spark_config()
    print(spark_config)

