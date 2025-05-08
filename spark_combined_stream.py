from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, round, to_timestamp, expr
from pyspark.sql.types import *

# ✅ Create Spark session
spark = SparkSession.builder \
    .appName("CombinedStreamWithIncidents") \
    .master("local[*]") \
    .getOrCreate()

# ✅ Schemas
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

incident_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("location", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True)
    ])),
    StructField("type", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("description", StringType(), True)
])

# ✅ Traffic Stream
df_traffic = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic-data") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), traffic_schema).alias("data")) \
    .select("data.*") \
    .withColumn("traffic_time", to_timestamp("timestamp")) \
    .withColumn("lat_round", round(col("location.latitude"), 1)) \
    .withColumn("lon_round", round(col("location.longitude"), 1)) \
    .withWatermark("traffic_time", "30 minutes")

# ✅ Weather Stream
df_weather = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather-data") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), weather_schema).alias("data")) \
    .select("data.*") \
    .withColumn("weather_time", to_timestamp("timestamp")) \
    .withColumn("lat_round", round(col("location.latitude"), 1)) \
    .withColumn("lon_round", round(col("location.longitude"), 1)) \
    .withWatermark("weather_time", "30 minutes")

# ✅ Incident Stream
df_incident = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic-incidents") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), incident_schema).alias("data")) \
    .select("data.*") \
    .withColumn("incident_time", to_timestamp("timestamp")) \
    .withColumn("lat_round", round(col("location.latitude"), 1)) \
    .withColumn("lon_round", round(col("location.longitude"), 1)) \
    .withWatermark("incident_time", "30 minutes")

# ✅ Step 1: Join traffic + weather
df_joined = df_traffic.join(
    df_weather,
    on=["lat_round", "lon_round"]
).where(
    (col("traffic_time") >= expr("weather_time - INTERVAL 15 MINUTES")) &
    (col("traffic_time") <= expr("weather_time + INTERVAL 15 MINUTES"))
)

# ✅ Step 2: Join the result with incident stream
df_final = df_joined.join(
    df_incident,
    on=["lat_round", "lon_round"]
).where(
    (col("traffic_time") >= expr("incident_time - INTERVAL 15 MINUTES")) &
    (col("traffic_time") <= expr("incident_time + INTERVAL 15 MINUTES"))
)

# ✅ Step 3: Select final output
df_output = df_final.select(
    df_traffic["timestamp"].alias("time"),
    "station_id", "freeway", "direction", "lane_type",
    col("traffic.flow").alias("flow"),
    col("traffic.occupancy").alias("occupancy"),
    col("traffic.speed").alias("speed"),
    "region", "temperature", "windspeed", "weathercode",
    col("type").alias("incident_type"),
    "severity", "description"
)

# ✅ Write to console
query = df_output.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("checkpointLocation", "file:///Users/spartan/Desktop/spark-checkpoints/final") \
    .start()

query.awaitTermination()
