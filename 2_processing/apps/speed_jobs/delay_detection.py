from shlex import split
import socket
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, when, regexp_extract, lit, broadcast, count, sum, avg, concat, date_format, to_timestamp, expr, date_add, unix_timestamp, split, lpad, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Ignore schema evolution → handles window changes

spark = SparkSession.builder \
    .appName("tcl-delay-detection") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false") \
    .getOrCreate()

schema = StructType([
    StructField("coursetheorique", StringType(), True),
    StructField("delaipassage", StringType(), True),
    StructField("direction", StringType(), True),
    StructField("gid", IntegerType(), True),
    StructField("heurepassage", TimestampType(), True),
    StructField("id", IntegerType(), True),
    StructField("idtarretdestination", IntegerType(), True),
    StructField("last_updated_fme", StringType(), True),
    StructField("ligne", StringType(), True),
    StructField("type", StringType(), True)
])

stops_schema = StructType([
    StructField("stop_id", IntegerType(), True),
    StructField("stop_name", StringType(), True),
    StructField("stop_lat", StringType(), True),
    StructField("stop_lon", StringType(), True)
])

stops_df = spark.read.schema(stops_schema).csv("/opt/spark-data/GTFS_TCL/stops.csv", header=True).cache()


stop_times_df = spark.read.csv("/opt/spark-data/GTFS_TCL/stop_times.txt", header=True, sep=",") \
  .select("trip_id", "arrival_time", "stop_id") \
  .join(broadcast(stops_df), "stop_id", "left") \
  .cache()

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-tcl:29092") \
    .option("subscribe", "tcl-passages") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

parsed = raw.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("kafka_ts")
).select("data.*", "kafka_ts")

# Join GTFS: coursetheorique = trip_id, idtarretdestination = stop_id
enriched = parsed.alias("s").join(
  stop_times_df.alias("g"), 
  (col("s.coursetheorique") == col("g.trip_id")) & (col("s.id") == col("g.stop_id")), 
  "left"  
)

# Build scheduled_ts: combine heurepassage DATE + scheduled_arrival TIME
enriched = enriched.withColumn(
  "scheduled_arrival_clean",
  when(
    regexp_extract(col("g.arrival_time"), r"^(\d+)", 1).cast("int") >= 24,
    concat(
      date_format(date_add(to_date(col("s.heurepassage")), 1), "yyyy-MM-dd"), lit(" "),
      lpad((split(col("g.arrival_time"), ":")[0].cast("int") - 24).cast("string"), 2, "0"),
      lit(":"), split(col("g.arrival_time"), ":")[1],
      lit(":"), split(col("g.arrival_time"), ":")[2]
    )
  ).otherwise(
    concat(date_format(col("s.heurepassage"), "yyyy-MM-dd"), lit(" "), col("g.arrival_time"))
  )
).withColumn("scheduled_ts", to_timestamp(col("scheduled_arrival_clean"), "yyyy-MM-dd HH:mm:ss")) \
 .withColumn("stop_name", col("g.stop_name")) 

# Delay: only meaningful when type = E (GPS estimated)

enriched = enriched.withColumn(
    "delay_minutes",
    when(
        (col("s.type") == "E") & col("scheduled_ts").isNotNull(),
        (unix_timestamp(col("s.heurepassage")) - unix_timestamp(col("scheduled_ts"))) / 60.0
    ).otherwise(lit(None))
)

enriched = enriched.withColumn(
    "is_late",
    when(col("delay_minutes") > 2, 1).otherwise(0)  # > 2 min threshold
)

# Windowed aggregation
delay_stats = enriched \
    .withWatermark("heurepassage", "10 minutes") \
    .groupBy(
        window(col("heurepassage"), "10 minutes").getField("start").alias("window_ts"),
        col("s.ligne"),
        col("s.direction"),
    ) \
    .agg(
        count("*").alias("total_passages"),
        sum("is_late").alias("late_count"),
        avg("delay_minutes").alias("avg_delay_min"),  # null-safe: ignores T rows
        count(when(col("delay_minutes") < -1, True)).alias("early_count")
    )

stop_delay_stats = enriched \
    .withWatermark("heurepassage", "10 minutes") \
    .groupBy(
        window(col("heurepassage"), "10 minutes").getField("start").alias("window_ts"),
        col("s.ligne"),
        col("s.direction"),
        col("g.stop_id").alias("stop_id")
        ) \
    .agg(
        count("*").alias("total_passages"),
        sum("is_late").alias("late_count"),
        avg("delay_minutes").alias("avg_delay_min"),  # null-safe: ignores T
        count(when(col("delay_minutes") < -1, True)).alias("early_count")
    )
# Write to Cassandra

query1 = delay_stats.writeStream \
  .outputMode("complete") \
  .format("org.apache.spark.sql.cassandra") \
  .option("keyspace", "tcl") \
  .option("table", "delay_stats_line") \
  .option("confirm.truncate", "true") \
  .option("checkpointLocation", "/app/checkpoint/line_stats") \
  .trigger(processingTime='1 minute') \
  .start()

# Stop stats
query2 = stop_delay_stats.writeStream \
  .outputMode("complete") \
  .format("org.apache.spark.sql.cassandra") \
  .option("keyspace", "tcl") \
  .option("table", "delay_stats_stop") \
  .option("confirm.truncate", "true") \
  .option("checkpointLocation", "/app/checkpoint/stop_stats") \
  .trigger(processingTime='1 minute') \
  .start()

query1.awaitTermination()
query2.awaitTermination()
