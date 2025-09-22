from confluent_kafka import Consumer
import json

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'weather-alert-consumer',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['weather_transformed'])

print("📡 En écoute sur le topic : weather_transformed")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Erreur : {msg.error()}")
            continue

        data = json.loads(msg.value().decode('utf-8'))
        print(f"🧾 Alerte reçue : {data}")

except KeyboardInterrupt:
    print("🛑 Arrêt du consommateur")
finally:
    consumer.close()
