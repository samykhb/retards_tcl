import json
import os
import time
from datetime import datetime
import pandas as pd
from kafka import KafkaConsumer

def main():
    topic = 'tcl-passages'
    batch_size = 10000
    output_dir = "./data/raw"

    # Creer le repertoire de destination
    os.makedirs(output_dir, exist_ok=True)

    # Initialiser le consommateur Kafka avec gestion de l'attente
    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers='kafka:29092',
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='batch-archiver-group',
                consumer_timeout_ms=1000
            )
            print("Connexion au broker Kafka reussie pour l'archivage.")
        except Exception:
            print("Kafka indisponible. Nouvelle tentative dans 5 secondes...")
            time.sleep(5)

    print("Demarrage de la creation des fichiers Parquet...")
    buffer = []

    try:
        while True:
            # Lire les messages disponibles
            for message in consumer:
                record = json.loads(message.value.decode('utf-8'))
                buffer.append(record)

                # Declencher l'ecriture lorsque le seuil est atteint
                if len(buffer) >= batch_size:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"tcl_batch_{timestamp}.parquet"
                    filepath = os.path.join(output_dir, filename)

                    df = pd.DataFrame(buffer).astype(str)
                    df.to_parquet(filepath, engine='pyarrow', index=False)
                    
                    print(f"Fichier genere : {filename} ({len(buffer)} lignes)")
                    buffer.clear()
            
            time.sleep(1)

    except KeyboardInterrupt:
        print("Interruption du processus d'archivage.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()