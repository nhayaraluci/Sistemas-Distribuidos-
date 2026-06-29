import os
import time
import json
import redis
from flask import Flask, request, jsonify
from kafka import KafkaProducer

app = Flask(__name__)

EVENTOS = []

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = "metrics-topic"


redis_cliente = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True
)

while True:
    try:
        redis_cliente.ping()
        print("[METRICAS] Redis OK ✔", flush=True)
        break
    except:
        print("[METRICAS] esperando Redis...", flush=True)
        time.sleep(2)


def crear_producer():
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=10
            )
            print("[METRICAS] Kafka OK ✔", flush=True)
            return p
        except Exception as e:
            print("[METRICAS] esperando Kafka...", repr(e), flush=True)
            time.sleep(3)

producer = crear_producer()


def normalizar(evento):
    evento = evento or {}

    evento.setdefault("tipo", "unknown")
    evento.setdefault("latencia", 0.0)
    evento.setdefault("retry_count", 0)
    evento.setdefault("service", "cache")
    evento.setdefault("timestamp", time.time())

    return evento



@app.route("/registrar", methods=["POST"])
def registrar():

    evento = request.json

   
    if not evento:
        print("[METRICAS] ERROR: evento vacío", flush=True)
        return jsonify({"ok": False}), 400

    
    print("\n================ METRICA RECIBIDA ================", flush=True)
    print("[FROM CACHE]:", json.dumps(evento, indent=2), flush=True)

    # 🔧 NORMALIZAR
    evento = normalizar(evento)

    
    print("\n[METRICAS] NORMALIZADO:", json.dumps(evento, indent=2), flush=True)

    EVENTOS.append(evento)

    try:
        future = producer.send(TOPIC, evento)
        metadata = future.get(timeout=10)

       
        print(
            f"\n[METRICAS → KAFKA] OK topic={metadata.topic} "
            f"partition={metadata.partition} offset={metadata.offset}",
            flush=True
        )

    except Exception as e:
        print("[METRICAS] ERROR KAFKA ", repr(e), flush=True)

    return jsonify({"ok": True})


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "eventos_recibidos": len(EVENTOS)
    })


if __name__ == "__main__":
    print("[METRICAS] SERVICIO INICIADO 🚀", flush=True)
    app.run(host="0.0.0.0", port=5002)