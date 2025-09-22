import sys
import requests
import json
from confluent_kafka import Producer

def create_producer():
    return Producer({
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'weather-producer'
    })

def get_weather_data(lat, lon):
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get("current_weather", {})
    except Exception as e:
        print(f"Erreur lors de la récupération météo : {e}")
        return {}

def delivery_callback(err, msg):
    if err:
        print(f"Erreur d'envoi : {err}")
    else:
        print(f"✅ Message envoyé à {msg.topic()} [partition {msg.partition()}] offset {msg.offset()}")

def main():
    if len(sys.argv) != 3:
        print("Usage : python current_weather.py <latitude> <longitude>")
        sys.exit(1)

    lat, lon = sys.argv[1], sys.argv[2]
    weather = get_weather_data(lat, lon)

    if not weather:
        print("Aucune donnée météo reçue.")
        return

    producer = create_producer()
    message = json.dumps(weather).encode('utf-8')

    producer.produce(
        topic='weather_stream',
        value=message,
        callback=delivery_callback
    )
    producer.flush()

if __name__ == "__main__":
    main()
