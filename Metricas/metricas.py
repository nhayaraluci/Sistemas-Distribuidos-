import os
import time
import redis
import json
from flask import Flask, request, jsonify
from kafka import KafkaProducer

app = Flask(__name__)

EVENTOS = []

CONFIG = {
    "politica": None,
    "tamano": None,
    "ttl": None
}

# =========================
# REDIS
# =========================

redis_cliente = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True
)

# Esperar Redis
while True:
    try:
        redis_cliente.ping()
        print("Redis conectado ✔", flush=True)
        break
    except Exception:
        print("Esperando Redis...", flush=True)
        time.sleep(2)

# =========================
# KAFKA
# =========================

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")


def crear_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=10
            )

            print("Kafka Producer conectado ✔", flush=True)
            return producer

        except Exception as e:
            print(f"Esperando Kafka... ({e})", flush=True)
            time.sleep(3)


producer = crear_producer()

# =========================
# NORMALIZAR EVENTOS
# =========================

def normalizar_evento(evento):

    tipo = evento.get("evento")

    if tipo in ["eviccion", "eviction"]:
        evento["evento"] = "eviction"

    return evento

# =========================
# ENDPOINT
# =========================

@app.route("/registrar", methods=["POST"])
def registrar():

    print("EVENTO RECIBIDO:", request.json, flush=True)

    evento = request.json or {}

    evento["timestamp"] = time.time()

    evento = normalizar_evento(evento)

    if "politica" in evento:
        CONFIG["politica"] = evento["politica"]

    if "tamano" in evento:
        CONFIG["tamano"] = evento["tamano"]

    if "ttl" in evento:
        CONFIG["ttl"] = evento["ttl"]

    EVENTOS.append(evento)

    try:
        future = producer.send("metrics-topic", evento)
        metadata = future.get(timeout=10)

        print(
        f"ENVIADO A KAFKA -> topic={metadata.topic}, partition={metadata.partition}, offset={metadata.offset}",
        flush=True
        )

    except Exception as e:
         print("ERROR KAFKA:", repr(e), flush=True)

    return jsonify({"ok": True})

# =========================
# HEALTH
# =========================

@app.route("/health")
def health():

    return jsonify({
        "estado": "ok",
        "eventos_recibidos": len(EVENTOS),
        "config": CONFIG
    })

# =========================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=5002
    )