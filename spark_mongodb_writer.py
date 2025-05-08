from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, round, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Step 1: Create SparkSession
spark = SparkSession.builder \
    .appName("MongoDBWriter") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/") \
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
    .getOrCreate()

# Step 2: Define schemas
traffic_schema = StructType([
    StructField("timestamp", StringType()),
    StructField("station_id", IntegerType()),
    StructField("location", StructType([
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType())
    ])),
    StructField("freeway", StringType()),
    StructField("direction", StringType()),
    StructField("lane_type", StringType()),
    StructField("traffic", StructType([
        StructField("flow", DoubleType()),
        StructField("occupancy", DoubleType()),
        StructField("speed", DoubleType())
    ]))
])

weather_schema = StructType([
    StructField("region", StringType()),
    StructField("timestamp", StringType()),
    StructField("location", StructType([
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType())
    ])),
    StructField("temperature", DoubleType()),
    StructField("windspeed", DoubleType()),
    StructField("winddirection", DoubleType()),
    StructField("weathercode", IntegerType())
])

incident_schema = StructType([
    StructField("timestamp", StringType()),
    StructField("type", StringType()),
    StructField("severity", StringType()),
    StructField("location", StructType([
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType())
    ])),
    StructField("description", StringType())
])

# Step 3: Read streams
traffic_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic-data") \
    .option("startingOffsets", "latest") \
    .load()

weather_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather-data") \
    .option("startingOffsets", "latest") \
    .load()

incident_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic-incidents") \
    .option("startingOffsets", "latest") \
    .load()

# Step 4: Parse Kafka JSON values
traffic_json = traffic_df.select(from_json(col("value").cast("string"), traffic_schema).alias("data")).select("data.*")
weather_json = weather_df.select(from_json(col("value").cast("string"), weather_schema).alias("data")).select("data.*")
incident_json = incident_df.select(from_json(col("value").cast("string"), incident_schema).alias("data")).select("data.*")

# Step 5: Preprocessing and Watermark
traffic_parsed = traffic_json.withColumn("traffic_time", to_timestamp("timestamp")).withWatermark("traffic_time", "15 minutes")
weather_parsed = weather_json.withColumn("weather_time", to_timestamp("timestamp")).withWatermark("weather_time", "15 minutes")
incident_parsed = incident_json.withColumn("incident_time", to_timestamp("timestamp")).withWatermark("incident_time", "15 minutes")

# Add rounded lat/lon for join (now rounding to 1 decimal for better matching)
traffic_ready = traffic_parsed.withColumn("traffic_lat_round", round(col("location.latitude"), 1)).withColumn("traffic_lon_round", round(col("location.longitude"), 1))
weather_ready = weather_parsed.withColumn("weather_lat_round", round(col("location.latitude"), 1)).withColumn("weather_lon_round", round(col("location.longitude"), 1))
incident_ready = incident_parsed.withColumn("incident_lat_round", round(col("location.latitude"), 1)).withColumn("incident_lon_round", round(col("location.longitude"), 1))

# Step 6: Traffic + Weather Join
traffic_weather_joined = traffic_ready.join(
    weather_ready,
    (traffic_ready.traffic_lat_round == weather_ready.weather_lat_round) &
    (traffic_ready.traffic_lon_round == weather_ready.weather_lon_round) &
    (traffic_ready.traffic_time.between(weather_ready.weather_time - expr("INTERVAL 15 MINUTES"), weather_ready.weather_time)),
    "inner"
)

# Step 7: TrafficWeather + Incident Join
final_joined = traffic_weather_joined.join(
    incident_ready,
    (traffic_weather_joined.traffic_lat_round == incident_ready.incident_lat_round) &
    (traffic_weather_joined.traffic_lon_round == incident_ready.incident_lon_round) &
    (traffic_weather_joined.traffic_time.between(incident_ready.incident_time - expr("INTERVAL 15 MINUTES"), incident_ready.incident_time)),
    "leftOuter"
)

# Step 8: Select final fields
final_df = final_joined.select(
    col("traffic_time").alias("timestamp"),
    col("station_id"),
    col("freeway"),
    col("direction"),
    col("lane_type"),
    col("traffic.flow").alias("flow"),
    col("traffic.occupancy").alias("occupancy"),
    col("traffic.speed").alias("speed"),
    col("region"),
    col("temperature"),
    col("windspeed"),
    col("weathercode"),
    col("type").alias("incident_type"),
    col("severity"),
    col("description")
)

# Step 9: Write to MongoDB (✅ with correct options now)
query = final_df.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "file:///Users/spartan/Desktop/spark-checkpoints/mongodb") \
    .option("database", "smart_traffic_db") \
    .option("collection", "traffic_weather_incidents") \
    .outputMode("append") \
    .start()

query.awaitTermination()




