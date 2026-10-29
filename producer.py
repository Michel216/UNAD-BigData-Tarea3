import time
import json
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("--- Transmitiendo mediciones de Calidad del Aire a Kafka ---")
estaciones = ["CIUDADELA", "FONTIBON", "KENNEDY"]
variables = ["DViento", "PM10", "O3"]

try:
    while True:
        data = {
            "estacion": random.choice(estaciones),
            "variable": random.choice(variables),
            "medicion": round(random.uniform(10.0, 350.0), 2),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        producer.send('calidad_aire_stream', value=data)
        print(f"Enviado a Kafka: {data}")
        time.sleep(2)
except KeyboardInterrupt:
    print("\nTransmisión finalizada.")
