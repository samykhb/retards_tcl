import socket
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, when, regexp_extract, lit, broadcast, count, sum, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Ignore schema evolution → handles window changes

def cassandra_available(host="cassandra", port=9042, timeout=3):
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
        return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False

spark = SparkSession.builder \
    .appName("tcl-delay-detection") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
    .config("spark.cassandra.connection.host", "10.129.193.134") \
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

routes_schema = StructType([
    StructField("ligne", StringType(), True),
    StructField("code_trace", StringType(), True),
    StructField("nom_trace", StringType(), True),
    StructField("sens", StringType(), True),
    StructField("nom_origine", StringType(), True),
    StructField("nom_destination", StringType(), True),
    StructField("pmr", StringType(), True)
])

routes_df = spark.read \
    .option("header", "true") \
    .option("sep", ";") \
    .schema(routes_schema) \
    .csv("/opt/spark-data/routes.csv") \
    .select("ligne", "code_trace", "nom_trace", "sens", "nom_origine", "nom_destination", "pmr") \
    .cache() 

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-tcl:29092") \
    .option("subscribe", "tcl-passages") \
    .option("startingOffsets", "latest") \
    .load()

parsed = raw.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("kafka_ts")
).select("data.*", "kafka_ts")

# Parse delai_minutes
parsed = parsed.withColumn("delai_minutes",
    when(col("delaipassage").rlike(r"^\d+\s*min$"), regexp_extract(col("delaipassage"), r"(\d+)", 1).cast("float"))
    .when(col("delaipassage").rlike(r"^\d+h\d+$"), regexp_extract(col("delaipassage"), r"^(\d+)h", 1).cast("float") * 60 + regexp_extract(col("delaipassage"), r"h(\d+)$", 1).cast("float"))
    .when(col("delaipassage") == "Proche", lit(0.5))
    .when(col("delaipassage") == "A l'arret", lit(0))
    .otherwise(None)
)

parsed = parsed.alias("rt").join(
    routes_df.alias("routes").hint("broadcast"),
    (col("rt.ligne") == col("routes.ligne")) &
    (col("rt.direction").contains(col("routes.nom_trace").substr(1, 50))),
    "left"
).select(
    col("rt.*"),
    col("routes.code_trace").alias("route_code"),
    col("routes.nom_trace").alias("route_name"),
    col("routes.nom_origine"),
    col("routes.nom_destination"),
    col("routes.pmr")
).drop("rt")

delay_stats = parsed \
    .withWatermark("heurepassage", "10 minutes") \
    .groupBy(
        window(col("heurepassage"), "10 minutes")
        .getField("start")
        .alias("window_ts"),
        col("ligne"), 
        col("direction"), 
        col("route_code"), 
        col("pmr")
    ) \
    .agg(
        count("*").alias("total_passages"),
        avg("delai_minutes").alias("avg_delay_min"),
        count(when(col("type") == "E", True)).alias("realtime_count")
    )

# Write to Cassandra
if cassandra_available():
    query = delay_stats.writeStream \
        .outputMode("complete") \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "tcl") \
        .option("table", "delay_stats") \
        .option("confirm.truncate", "true") \
        .option("checkpointLocation", "/app/checkpoint/delay_stats") \
        .trigger(processingTime='1 minute') \
        .start()
else :
    query = delay_stats.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='1 minute') \
        .start()
query.awaitTermination()
