from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Step 1: Start Spark session
spark = SparkSession.builder \
    .appName("TrafficStream") \
    .master("local[*]") \
    .getOrCreate()

# Step 2: Traffic data schema
traffic_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("station_id", IntegerType(), True),
    StructField("location", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True)
    ])),
    StructField("freeway", StringType(), True),
    StructField("direction", StringType(), True),
    StructField("lane_type", StringType(), True),
    StructField("traffic", StructType([
        StructField("flow", DoubleType(), True),
        StructField("occupancy", DoubleType(), True),
        StructField("speed", DoubleType(), True)
    ]))
])

# Step 3: Weather data schema
weather_schema = StructType([
    StructField("region", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("location", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True)
    ])),
    StructField("temperature", DoubleType(), True),
    StructField("windspeed", DoubleType(), True),
    StructField("winddirection", DoubleType(), True),
    StructField("weathercode", IntegerType(), True)
])
# Step 4: accident data schema
incident_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("type", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("location", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True)
    ])),
    StructField("description", StringType(), True)
])

# Step 5: Read traffic stream from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic-data") \
    .option("startingOffsets", "latest") \
    .load()

df_json = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), traffic_schema).alias("data")) \
    .select("data.*")

# Step 6: Read weather stream from Kafka
df_weather_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather-data") \
    .option("startingOffsets", "latest") \
    .load()

df_weather_json = df_weather_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), weather_schema).alias("data")) \
    .select("data.*")

# Step 7: Read accident stream from Kafka
df_incident_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic-incidents") \
    .option("startingOffsets", "latest") \
    .load()

df_incident_json = df_incident_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), incident_schema).alias("data")) \
    .select("data.*")


# Step 8: Output traffic data to console
query = df_json.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("checkpointLocation", "file:///Users/spartan/Desktop/spark-checkpoints/traffic") \
    .start()

# Step 9: Output weather data to console
query_weather = df_weather_json.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("checkpointLocation", "file:///Users/spartan/Desktop/spark-checkpoints/weather") \
    .start()

# Step 9: Output accident data to console
query_incident = df_incident_json.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("checkpointLocation", "file:///Users/spartan/Desktop/spark-checkpoints/incidents") \
    .start()


# Step 8: Keep both streams alive
query.awaitTermination()
query_weather.awaitTermination()
query_incident.awaitTermination()
