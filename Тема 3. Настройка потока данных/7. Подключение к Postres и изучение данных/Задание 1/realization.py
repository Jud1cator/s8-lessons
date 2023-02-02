from pyspark.sql import SparkSession


spark = (
    SparkSession.builder.appName('test_postgres_connection')
    .config(
        "spark.jars.packages", "org.postgresql:postgresql:42.4.0"
    )
    .getOrCreate()
)


df = (
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


df.count()
