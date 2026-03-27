from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, round, avg, count, split, hour, minute, second,
    when, sum, current_timestamp
)


def main():
    spark = SparkSession.builder \
        .appName("TCL_Batch_Layer") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") \
        .config("spark.hadoop.fs.s3a.listing.cache.expiration", "5000") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Chemins des données
    input_path_api = "s3a://tcl-datalake/year=2026/month=03/"
    path_stops = "/opt/spark-apps/stops.txt"
    path_stop_times = "/opt/spark-apps/stop_times.txt"

    print("Demarrage du traitement Batch Spark...")

    try:
        # Chargement des fichiers statiques
        df_stops = spark.read.csv(path_stops, header=True, inferSchema=True)
        df_times = spark.read.csv(path_stop_times, header=True, inferSchema=True)

        # Chargement de l'API (MinIO) et filtre sur type = Estimé
        df_api = spark.read.parquet(input_path_api).dropDuplicates(["gid"])
        df_live = df_api.filter(col("type") == "E")

        # Jointures avec les fichiers statiques
        df_joined = df_live.join(
            df_times,
            (df_live.coursetheorique == df_times.trip_id) & (df_live.id == df_times.stop_id),
            "inner"
        )
        df_final = df_joined.join(df_stops, df_joined.id == df_stops.stop_id, "inner")

        # Calcul du retard en secondes depuis minuit
        df_calc = df_final.withColumn(
            "api_sec_midnight",
            hour("heurepassage") * 3600 + minute("heurepassage") * 60 + second("heurepassage")
        )
        split_time = split(col("arrival_time"), ":")
        df_calc = df_calc.withColumn(
            "gtfs_sec_midnight",
            split_time.getItem(0).cast("int") * 3600
            + split_time.getItem(1).cast("int") * 60
            + split_time.getItem(2).cast("int")
        )
        df_calc = df_calc.withColumn(
            "delay_min",
            round((col("api_sec_midnight") - col("gtfs_sec_midnight")) / 60.0, 2)
        )

        # Agrégation
        df_stats = df_calc.groupBy("ligne", "direction", "stop_name") \
            .agg(
                count("*").alias("total_passages"),
                sum(when(col("delay_min") > 0, 1).otherwise(0)).alias("late_count"),
                round(avg("delay_min"), 2).alias("avg_delay_min")
            )
        df_stats = df_stats.withColumn("window_ts", current_timestamp())

        # Écriture Cassandra
        df_stats.write.format("org.apache.spark.sql.cassandra") \
            .options(table="batch_delay_stats", keyspace="tcl") \
            .mode("append").save()

        print("Batch termine avec succes !")
    except Exception as e:
        print(f"Erreur Batch : {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
