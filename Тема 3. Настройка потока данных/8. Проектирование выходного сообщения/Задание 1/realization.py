from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType


def spark_init(test_name) -> SparkSession:
    return (
        SparkSession.builder.appName(test_name)
        .config(
            "spark.jars.packages",
            ",".join([
                "org.postgresql:postgresql:42.4.0",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
            ])
        )
        .getOrCreate()
    )


postgresql_settings = {
    'user': 'master',
    'password': 'de-master-password'
}


def read_marketing(spark: SparkSession) -> DataFrame:
    return (
        spark
        .read
        .jdbc(
            "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de",
            "public.marketing_companies",
            properties={
                "user": "student",
                "password": "de-student",
                "driver": "org.postgresql.Driver"
            }
        )
    )


kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka-admin\" password=\"de-kafka-admin-2022\";',
}


def read_client_stream(spark: SparkSession) -> DataFrame:
    schema = StructType([
        StructField("client_id", StringType()),
        StructField("timestamp", DoubleType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
    ])

    return (
        spark.readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
        .options(**kafka_security_options)
        .option("subscribe", "student.topic.cohort6.jud1cator")
        .load()
        .withColumn('value', f.col('value').cast(StringType()))
        .withColumn('event', f.from_json(f.col('value'), schema))
        .selectExpr('event.*')
        .withColumn(
            'timestamp',
            f.from_unixtime(f.col('timestamp'), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(TimestampType())
        )
        .dropDuplicates(['client_id', 'timestamp'])
        .withWatermark('timestamp', '5 minute')
    )


def join(user_df, marketing_df) -> DataFrame:
    return (
        user_df.crossJoin(marketing_df)
    )


if __name__ == "__main__":
    spark = spark_init('join stream')
    client_stream = read_client_stream(spark)
    marketing_df = read_marketing(spark)
    result = join(client_stream, marketing_df)

    query = (result
             .writeStream
             .outputMode("append")
             .format("console")
             .option("truncate", False)
             .start())
    query.awaitTermination()
