import json
import os
import time
from datetime import datetime
import pandas as pd
from kafka import KafkaConsumer
import s3fs

def main():
    topic = 'tcl-passages'
    batch_size = 10000
    
    # Configuration MinIO via variables d'environnement
    s3_endpoint = os.getenv('S3_ENDPOINT', 'http://minio:9000')
    s3_key = os.getenv('AWS_ACCESS_KEY_ID', 'admin')
    s3_secret = os.getenv('AWS_SECRET_ACCESS_KEY', 'password123')
    bucket_name = "tcl-datalake"

    # Initialiser le système de fichiers S3
    fs = s3fs.S3FileSystem(
        key=s3_key,
        secret=s3_secret,
        endpoint_url=s3_endpoint
    )

    if not fs.exists(bucket_name):
        fs.mkdir(bucket_name)
        print(f"Bucket '{bucket_name}' créé.")

    # Initialiser le consumer Kafka
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
            print("Connexion au broker Kafka reussie pour l'archivage S3.")
        except Exception:
            print("Kafka indisponible. Nouvelle tentative dans 5 secondes...")
            time.sleep(5)

    print(f"Demarrage de l'archivage vers s3://{bucket_name} ...")
    buffer = []

    try:
        while True:
            for message in consumer:
                record = json.loads(message.value.decode('utf-8'))
                buffer.append(record)

                if len(buffer) >= batch_size:
                    # Organisation par date (partitionnement pour Spark plus tard)
                    now = datetime.now()
                    timestamp = now.strftime("%H%M%S")
                    date_path = now.strftime("year=%Y/month=%m/day=%d/hour=%H")
                    
                    filename = f"tcl_batch_{timestamp}.parquet"
                    # Chemin complet S3
                    s3_path = f"{bucket_name}/{date_path}/{filename}"

                    # Conversion et envoi
                    df = pd.DataFrame(buffer).astype(str)
                    
                    df.to_parquet(
                        f"s3://{s3_path}", 
                        index=False,
                        storage_options={
                            "key": s3_key,
                            "secret": s3_secret,
                            "client_kwargs": {"endpoint_url": s3_endpoint}
                        }
                    )
                    
                    print(f"Parquet envoyé sur MinIO : {s3_path} ({len(buffer)} lignes)")
                    buffer.clear()
            
            time.sleep(1)

    except KeyboardInterrupt:
        print("Interruption de l'archivage.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()