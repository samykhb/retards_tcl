from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, avg, count, max, min, countDistinct, stddev, regexp_extract
from cassandra.cluster import Cluster

def setup_cassandra_schema():
    print("Initialisation de Cassandra en cours...")
    try:
        # Connexion au cluster Cassandra
        clstr = Cluster(['cassandra'])
        session = clstr.connect()
        
        # Creation du Keyspace (TP6)
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS tcl 
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """)
        
        # Creation de la table batch
        session.execute("""
            CREATE TABLE IF NOT EXISTS tcl.stats_historiques (
                ligne text,
                direction text,
                attente_moyenne_min float,
                attente_max_min float,
                attente_min_min float,
                ecart_type_attente float,
                total_enregistrements int,
                PRIMARY KEY (ligne, direction)
            )
        """)
        
        # Creation de la table streaming
        session.execute("""
            CREATE TABLE IF NOT EXISTS tcl.retards_live (
                id text PRIMARY KEY,
                ligne text,
                direction text,
                retard_minutes float,
                heurepassage timestamp
            )
        """)
        print("Schema Cassandra pret.")
        session.shutdown()
        clstr.shutdown()
    except Exception as e:
        print(f"Erreur lors de la creation du schema Cassandra : {e}")

def main():
    # Preparer la base de donnees
    setup_cassandra_schema()

    # Configurer Spark avec le connecteur Cassandra
    spark = SparkSession.builder \
        .appName("TCL_Batch_Layer") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://tcl_minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Debuggage
    print("="*30)
    input_path = "s3a://tcl-datalake/year=2026/month=03/*/*/*" 
    print(f"Lecture des données depuis MinIO : {input_path}")
    print("="*30)
    
    print("Demarrage du traitement Batch Spark...")
    
    try:
        # Lecture depuis MinIO au lieu du dossier local
        df = spark.read.parquet(input_path)
        
        # On enlève les doublons
        df = df.dropDuplicates(["id", "last_update_fme"])

        # On ne garde que les données "Estimées" (vrai live)
        df_live = df.filter((col("type") == "E") & col("delaipassage").isNotNull())
        
        # Extraction du délai (temps d'attente à l'arrêt avant prochain passage)
        df_minutes = df_live.withColumn(
            "attente_min", 
            regexp_extract(col("delaipassage"), r"(\d+)", 1).cast("float")
        )       
        
        # On ne garde que les bus proches (temps d'attente <= 15 min)
        df_final = df_minutes.filter(col("attente_min") <= 15)
        
        df_stats = df_final.groupBy("ligne", "direction") \
            .agg(
                round(avg("attente_min"), 2).alias("attente_moyenne_min"), # attente moyenne en min, tous arrêts confondus
                max("attente_min").alias("attente_max_min"), # attente max en min
                min("attente_min").alias("attente_min_min"), # attente min en min
                round(stddev("attente_min"), 2).alias("ecart_type_attente"), # régularité
                count("*").alias("total_enregistrements") # nb total d'enregistrements
            )
        
        # Au cas où l'ecart type renvoie null (ex : 1 seul bus), on le remplace par 0
        df_stats = df_stats.fillna(0.0, subset=["ecart_type_attente"])

        print("\n--- Sauvegarde des resultats dans Cassandra ---")
        
        # Ecriture dans Cassandra
        df_stats.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="stats_historiques", keyspace="tcl") \
            .mode("append") \
            .save()
            
        print("Ecriture terminee avec succes !")
        
    except Exception as e:
        print(f"Erreur lors de l'execution : {e}")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()