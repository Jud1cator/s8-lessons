from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql.functions import split
from pyspark.sql.types import StructType, StructField, StringType

#необходимая библиотека для интеграции Spark и Postgres
spark_jars_packages = ",".join(
        [
            "org.postgresql:postgresql:42.4.0",
        ]
    )

#создаём SparkSession и передаём библиотеку для работы с Postgres
spark = SparkSession.builder \
    .appName("join data") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

#вычитываем данные из таблицы
tableDF = spark.read \
                    .format('jdbc') \
                    .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
                    .option('driver', 'org.postgresql.Driver') \
                    .option('dbtable', 'words') \
                    .option('user', 'login') \
                    .option('password', 'password') \
                    .load()

#определяем схему для датафрейма
userSchema = StructType([StructField("text", StringType(), True)])

#читаем текст из файла
wordsDF = spark.readStream.schema(userSchema).format('text').load('/datas8')

#разделяем слова по запятым
splitWordsDF = wordsDF.select(explode(split('text', ','))).alias('words')

#объединяем данные. Присоединяем данные из таблицы к данным из файла — join(...,...,...)
joinDF = splitWordsDF.join(tableDF, 'words', 'left')

#проверяем, каких слов нет в таблице, но какие есть в файле filter(...isNull())
#возвращаем только один столбец (select(...))
#убираем дубли (distinct())
filterDF = joinDF.filter(col('tableDF.words').isNull()).select('splitWordsDF.words').distinct()

#запускаем стриминг
filterDF.writeStream \
    .format("console") \
    .start() \
    .awaitTermination()
