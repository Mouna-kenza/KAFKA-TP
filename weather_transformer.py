from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, when, to_json, struct
from pyspark.sql.types import StructType, DoubleType

schema = StructType() \
    .add("temperature", DoubleType()) \
    .add("windspeed", DoubleType())

spark = SparkSession.builder.appName("WeatherTransformer").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_stream") \
    .load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

df_transformed = df_parsed \
    .withColumn("event_time", current_timestamp()) \
    .withColumn("wind_alert_level", when(col("windspeed") < 10, "level_0")
                .when((col("windspeed") <= 20), "level_1")
                .otherwise("level_2")) \
    .withColumn("heat_alert_level", when(col("temperature") < 25, "level_0")
                .when((col("temperature") <= 35), "level_1")
                .otherwise("level_2"))

df_output = df_transformed.select(to_json(struct("*")).alias("value"))

df_output.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "weather_transformed") \
    .option("checkpointLocation", "/tmp/spark-checkpoint-weather") \
    .start() \
    .awaitTermination()
