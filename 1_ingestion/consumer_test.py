import time
import json
from datetime import datetime
from kafka import KafkaAdminClient, KafkaConsumer

def consumer_from_kafka(topic):
    passages_connus = {}

    # Connexion au cluster Kafka
    consumer = KafkaConsumer(
        topic, 
        bootstrap_servers='kafka:29092',
        auto_offset_reset="earliest",
        enable_auto_commit=True, 
        auto_commit_interval_ms=1000
    )
    
    print(f"En ecoute sur le topic : {topic}")

    # Lecture continue des messages
    for message in consumer:
        passage = json.loads(message.value.decode('utf-8'))
        
        passage_id = passage.get("id")
        ligne = passage.get("ligne")
        direction = passage.get("direction")
        delai = passage.get("delaipassage")
        heure = passage.get("heurepassage")
            
        # Logique de detection des changements d'etat
        if passage_id not in passages_connus:
            passages_connus[passage_id] = delai
            print(f"NOUVEAU | Ligne {ligne:^4} vers {direction:^20} | Dans {delai} (prevu a {heure})")
            
        else:
            ancien_delai = passages_connus[passage_id]
            if ancien_delai != delai:
                passages_connus[passage_id] = delai
                print(f"MAJ     | Ligne {ligne:^4} vers {direction:^20} | Delai : {ancien_delai} -> {delai}")
    
    consumer.close()

def main():
    topic = 'tcl-passages'
    consumer_from_kafka(topic)

if __name__ == "__main__":
    main()