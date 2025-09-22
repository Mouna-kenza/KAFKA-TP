import sys
from kafka import KafkaConsumer

def main():
    if len(sys.argv) != 2:
        print("Usage : python consumer.py <nom_du_topic>")
        sys.exit(1)

    topic = sys.argv[1]

    # Création du consommateur Kafka
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='groupe-consommateur',
        enable_auto_commit=True
    )

    print(f"📡 En écoute sur le topic : {topic}\n")

    # Affichage des messages reçus en temps réel
    for message in consumer:
        print(f"Message reçu : {message.value.decode('utf-8')}")

if __name__ == "__main__":
    main()
