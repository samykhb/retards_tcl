from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, round, avg, desc
from cassandra.cluster import Cluster

def setup_cassandra_schema():
    print("Initialisation de Cassandra en cours...")
    try:
        # Connexion au cluster Cassandra
        clstr = Cluster(['cassandra'])
        
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
                retard_moyen_min float,
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
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    input_path = "/app/data/raw/*.parquet"
    
    print("Demarrage du traitement Batch Spark...")
    
    try:
        df = spark.read.parquet(input_path)
        df_clean = df.filter(col("heurepassage").isNotNull() & col("coursetheorique").isNotNull())
        
        df_retards = df_clean.withColumn(
            "retard_minutes", 
            (unix_timestamp(col("heurepassage")) - unix_timestamp(col("coursetheorique"))) / 60
        )
        
        df_filtre = df_retards.filter((col("retard_minutes") > -10) & (col("retard_minutes") < 120))
        
        df_stats = df_filtre.groupBy("ligne", "direction") \
            .agg(
                round(avg("retard_minutes"), 2).alias("retard_moyen_min")
            )
            
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