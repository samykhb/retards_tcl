from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, round, avg, desc, count, to_timestamp, regexp_extract
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
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Debuggage
    import os
    print("="*30)
    path_to_check = "/app/data/raw"
    if os.path.exists(path_to_check):
        print(f"Le dossier {path_to_check} existe.")
        print(f"Fichiers trouvés : {os.listdir(path_to_check)}")
    else:
        print(f"Ereur : Le dossier {path_to_check} est introuvable dans le container.")
    print("="*30)
    input_path = "/app/data/raw"
    
    print("Demarrage du traitement Batch Spark...")
    
    try:
        df = spark.read.parquet(input_path)
        
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
                round(avg("attente_min"), 2).alias("attente_moyenne_min"),
                count("*").alias("total_enregistrements")
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