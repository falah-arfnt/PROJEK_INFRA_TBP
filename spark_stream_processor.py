import os

os.environ["HADOOP_HOME"] = "D:\\hadoop"
os.environ["hadoop.home.dir"] = "D:\\hadoop"

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("MusicTrendPipeline") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4"
    ) \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "music-tracks") \
    .option("startingOffsets", "latest") \
    .load()

json_df = df_raw.selectExpr("CAST(value AS STRING) as json_data")
schema = StructType([
    StructField("song", StringType(), True),
    StructField("artist", StringType(), True),
    StructField("listeners", IntegerType(), True),
    StructField("playcount", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

music_df = json_df.select(
    from_json(col("json_data"), schema).alias("data")
)

music_df = music_df.select("data.*")
music_df = music_df.na.drop()
result_df = music_df.withColumn(
    "engagement_score",
    round(col("playcount") / col("listeners"), 2)
)
query = result_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

query.awaitTermination()