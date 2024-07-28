from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, FloatType, StringType, IntegerType, DateType
from pyspark.sql.functions import from_json, col

# Define the write interval to sink (cassandra)
SINK_WRITE_INTERVAL = "30 seconds"

# The data schema to be received from kafka
eventSchema = StructType([
    StructField("name", StringType(), False),
    StructField("song", StringType(), False),
    StructField("event_timestamp", StringType(), False)
    ])

# The data schema of the spotify-songs csv
songSchema = StructType([
    StructField("name", StringType(), False),
    StructField("artists", StringType(), False),
    StructField("duration_ms", IntegerType(), False),
    StructField("album_name", StringType(), False),
    StructField("album_release_date", DateType(), False),
    StructField("danceability", FloatType(), False),
    StructField("energy", FloatType(), False),
    StructField("key", StringType(), False),
    StructField("loudness", FloatType(), False),
    StructField("mode", StringType(), False),
    StructField("speechiness", FloatType(), False),
    StructField("acousticness", FloatType(), False),
    StructField("instrumentalness", FloatType(), False),
    StructField("liveness", FloatType(), False),
    StructField("valence", FloatType(), False),
    StructField("tempo", FloatType(), False)
    ])

# The spark session
spark = SparkSession.builder.appName("Spark streaming using Kafka and Cassandra")\
  .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0").\
    getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read (and cache) the spotify-songs csv as a dataframe
spotify_songs_df = spark.read.format("csv").option("header", True).schema(songSchema).load("spotify-songs.csv").\
  withColumnRenamed("name", "song_name").cache()

# The streaming dataframe from kafka
streaming_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:29092").option("subscribe", "spotify").option("startingOffsets", "latest").load() 

# The structured dataframe
sdf = streaming_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), eventSchema).alias("data")).select("data.*")

# Transform the event_timestamp into a Long (due to milliseconds precision)
transformed_df = sdf.withColumn("event_time",sdf.event_timestamp.cast(LongType())).drop("event_timestamp")

# Join the stream with the spotfy-songs dataframe
with_join_df = transformed_df.join(spotify_songs_df, transformed_df.song == spotify_songs_df.song_name, "inner")

# The dataframe to be sent to Cassandra
records_df = with_join_df.drop("song")

# Write the dataframe to cassandra
def writeToCassandra(writeDF, _):
  writeDF.write.format("org.apache.spark.sql.cassandra").mode('append').options(table="records", keyspace="spotify").save()

result = None

while result is None:
    try:
        # connect
        result = records_df.writeStream.option("spark.cassandra.connection.host","localhost:9042").\
          option("failOnDataLoss", "false").foreachBatch(writeToCassandra).\
          trigger(processingTime=SINK_WRITE_INTERVAL).\
          outputMode("update").start().awaitTermination()
    except:
         pass
