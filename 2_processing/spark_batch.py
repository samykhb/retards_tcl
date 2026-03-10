from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, round, avg, desc

def main():
    # Initialiser la session Spark avec des limites de memoire (8 gb de ram c chaud)
    spark = SparkSession.builder \
        .appName("TCL_Batch_Layer") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()

    # Reduire le niveau de log pour eviter de polluer la console
    spark.sparkContext.setLogLevel("WARN")

    input_path = "/app/data/raw/*.parquet"
    
    print("Demarrage du traitement Batch Spark...")
    
    try:
        # Charger les fichiers Parquet
        df = spark.read.parquet(input_path)
        
        # Conserver uniquement les lignes possedant les heures de passage theoriques et reelles
        df_clean = df.filter(col("heurepassage").isNotNull() & col("coursetheorique").isNotNull())
        
        # Calculer le retard en minutes (Conversion des timestamps en secondes, soustraction, puis division par 60)
        df_retards = df_clean.withColumn(
            "retard_minutes", 
            (unix_timestamp(col("heurepassage")) - unix_timestamp(col("coursetheorique"))) / 60
        )
        
        # Exclure les valeurs aberrantes dues aux erreurs de l'API (avance > 10 min ou retard > 120 min)
        df_filtre = df_retards.filter((col("retard_minutes") > -10) & (col("retard_minutes") < 120))
        
        print("\n--- Classement des pires retards moyens par ligne ---")
        
        # Grouper par ligne et direction, calculer la moyenne du retard, et trier par ordre decroissant
        df_stats = df_filtre.groupBy("ligne", "direction") \
            .agg(
                round(avg("retard_minutes"), 2).alias("retard_moyen_min")
            ) \
            .orderBy(desc("retard_moyen_min"))
            
        # Afficher les 20 premieres lignes
        df_stats.show(20, truncate=False)
        
    except Exception as e:
        print(f"Erreur lors de l'execution du job Spark : {e}")
        
    finally:
        # Arreter proprement le contexte Spark
        spark.stop()

if __name__ == "__main__":
    main()