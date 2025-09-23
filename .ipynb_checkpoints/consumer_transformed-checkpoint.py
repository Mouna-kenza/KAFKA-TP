from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Créer la session Spark
spark = SparkSession.builder.appName("WeatherConsumer").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Lire les données du topic Kafka weather_transformed
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather_transformed") \
    .load()

# Extraire le champ value et le convertir en chaîne
df_parsed = df.selectExpr("CAST(value AS STRING)")

# Afficher les données dans le terminal
df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start() \
    .awaitTermination()
