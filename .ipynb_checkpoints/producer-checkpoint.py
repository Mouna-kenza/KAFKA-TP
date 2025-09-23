from confluent_kafka import Producer
import json

def create_producer():
    """Crée un producteur Kafka avec gestion d'erreurs"""
    try:
        producer = Producer({
            'bootstrap.servers': 'localhost:9092',
            'client.id': 'python-producer'
        })
        return producer
    except Exception as e:
        print(f"Erreur lors de la création du producteur : {e}")
        return None

def delivery_callback(err, msg):
    """Callback appelé après l'envoi du message"""
    if err is not None:
        print(f"Erreur lors de l'envoi : {err}")
    else:
        print(f"Message envoyé avec succès !")
        print(f"Topic: {msg.topic()}")
        print(f"Partition: {msg.partition()}")
        print(f"Offset: {msg.offset()}")

def send_message():
    """Envoie un message vers le topic weather_stream"""
    producer = create_producer()
    
    if producer is None:
        return
    
    # Message à envoyer
    message = {"msg": "Hello Kafka"}
    message_json = json.dumps(message)
    
    try:
        # Envoi du message
        producer.produce(
            'weather_stream', 
            value=message_json.encode('utf-8'),
            callback=delivery_callback
        )
        
        # Attendre que le message soit envoyé
        producer.flush()
        print(f"Message: {message}")
        
    except Exception as e:
        print(f"Erreur lors de l'envoi : {e}")

if __name__ == "__main__":
    print("Démarrage du producteur Kafka...")
    send_message()