import logging
import os
logging.basicConfig(level=logging.DEBUG)
import json
import time
import urllib.request
import base64
from dotenv import load_dotenv  
from datetime import datetime
from kafka import KafkaProducer, KafkaClient 
from kafka.admin import KafkaAdminClient, NewTopic
    
load_dotenv()

def main():
    topic = 'tcl-passages'
    num_partition = 8
    
    # Identifiants Data Grand Lyon
    email = os.environ.get("TCL_EMAIL")
    password = os.environ.get("TCL_PASSWORD")

    url = "https://data.grandlyon.com/fr/datapusher/ws/rdata/tcl_sytral.tclpassagearret/all.json?maxfeatures=-1&start=1&filename=prochains-passages-reseau-transports-commun-lyonnais-rhonexpress-disponibilites-temps-reel"

    # Encodage de l'authentification
    auth_string = f"{email}:{password}"
    auth_base64 = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')

    # Initialisation du client admin Kafka avec attente (Retry), à cause d'erreurs répétées
    admin = None
    while admin is None:
        try:
            admin = KafkaAdminClient(bootstrap_servers='kafka-tcl:29092')
            print("Connexion a Kafka reussie !")
        except Exception as e:
            print(f"Kafka error: {type(e).__name__}: {str(e)}")
            print(f"Full traceback: {repr(e)}")
            time.sleep(5)

    server_topics = admin.list_topics()

    # Creation du topic s'il n'existe pas
    if topic not in server_topics:
        try:
            print(f"Creation du topic : {topic}")
            new_topic = NewTopic(name=topic, num_partitions=num_partition, replication_factor=1)
            admin.create_topics([new_topic])
        except Exception as e:
            print(f"Erreur lors de la creation du topic : {e}")
    else:
        print(f"Le topic {topic} est deja existant.")
    # Wrap producer init just like you did for admin
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(
            bootstrap_servers='kafka-tcl:29092',
            key_serializer=lambda k: k.encode('utf-8'),    # ← KEY SUPPORT
            value_serializer=lambda v: v.encode('utf-8'),
            acks='all',                    # wait for replicas
            batch_size=16384,              # batch messages
            linger_ms=100,                 # slight delay for batching
            retries=3,                     # retry failed sends
            compression_type='snappy'      # smaller on wire
            )
            print("Producteur Kafka initialisé !")
        except Exception as e:
            print(f"Producteur non disponible : {e}, nouvelle tentative dans 5s...")
            time.sleep(5)

    print("Demarrage de l'ingestion des donnees TCL...")

    # Boucle d'ingestion continue
    while True:
        try:
            req = urllib.request.Request(url)
            req.add_header("Authorization", f"Basic {auth_base64}")
            
            response = urllib.request.urlopen(req)
            data = json.loads(response.read().decode())
            
            passages = data.get("values", [])
            
            for passage in passages:
                key = passage.get('ligne', str(passage.get('gid', 0)))  # ligne OR gid
                producer.send(topic, key=key.encode('utf-8'), 
                value=json.dumps(passage).encode('utf-8'))
                
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {len(passages)} enregistrements envoyes.")
            
        except urllib.error.HTTPError as e:
            print(f"Erreur HTTP : {e.code} - {e.reason}")
        except Exception as e:
            print(f"Erreur d'execution : {e}")
            
        # Attente de 60 secondes avant la prochaine requete
        time.sleep(60)

if __name__ == "__main__":
    main()