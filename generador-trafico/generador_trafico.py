import time
import random
import json
import uuid
from kafka import KafkaProducer

time.sleep(10)

KAFKA_BROKER = "kafka:9092"
TOPIC = "queries" # nombre en donde se envia 

PROB_BASURA = 0.15
INTERVALO = 0.8

ZONAS = ["Z1", "Z2", "Z3", "Z4", "Z5"]

CONSULTAS_FRECUENTES = [
    ("Q1", "Z1", 0.5),
    ("Q1", "Z2", 0.5),
    ("Q2", "Z1", 0.5),
    ("Q3", "Z1", 0.5),
    ("Q3", "Z2", 0.5),
]

OPERACIONES = ["Q1", "Q2", "Q3", "Q4", "Q5"]


producer = None

while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=10
        )
        print("Kafka conectado ")
    except Exception as e:
        print("Esperando Kafka...", e)
        time.sleep(3)


def zipf_choice(items):
    weights = [1 / (i + 1) for i in range(len(items))]
    total = sum(weights)
    return random.choices(items, weights=[w / total for w in weights])[0]

def generar_basura():
    return {
        "request_id": str(uuid.uuid4()),
        "operacion": random.choice(["Q99", "INVALID", ""]),
        "zona_id": random.choice(ZONAS)
    }

def generar_consulta():

    if random.random() < PROB_BASURA:
        return generar_basura()

    if random.random() < 0.8:
        op, zona, conf = random.choice(CONSULTAS_FRECUENTES)
        return {
            "request_id": str(uuid.uuid4()),
            "operacion": op,
            "zona_id": zona,
            "confidence_min": conf
        }

    op = random.choice(OPERACIONES)
    zona = zipf_choice(ZONAS)

    q = {
        "request_id": str(uuid.uuid4()),
        "operacion": op,
        "zona_id": zona,
        "confidence_min": random.choice([0.3, 0.5, 0.7])
    }

    if op == "Q4":
        otros = [z for z in ZONAS if z != zona]
        q["zona_id_a"] = zona
        q["zona_id_b"] = random.choice(otros)

    if op == "Q5":
        q["bins"] = random.choice([5, 10, 15])

    return q



print(" GENERADOR DE TRFICO")

for i in range(10):

    consulta = generar_consulta()

    print(f"Enviando consulta {i+1} → {consulta['request_id']} | {consulta['operacion']}")

    producer.send(TOPIC, consulta)
    producer.flush()

    time.sleep(INTERVALO)

