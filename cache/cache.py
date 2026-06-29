import os
import json
import time
import redis
import threading
import requests
from collections import OrderedDict
from kafka import KafkaConsumer, KafkaProducer

print("SERVICIO DE CACHE INICIADO", flush=True)


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

POLITICA = os.getenv("POLITICA", "LRU")
TAMANO = int(os.getenv("TAMANO", 5))
TTL = int(os.getenv("TTL", 60))

MAX_RETRIES = 2
TIEMPO_REINTENTO = 5

URL_METRICAS = os.getenv("URL_METRICAS", "http://metricas:5002/registrar")

hits = 0
misses = 0
dlq_count = 0
retry_count_global = 0

lock_metrics = threading.Lock()


redis_cli = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True
)

while True:
    try:
        redis_cli.ping()
        break
    except:
        time.sleep(2)

print("REDIS OK", flush=True)


def crear_consumer(topic, group):
    while True:
        try:
            return KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id=group
            )
        except:
            time.sleep(3)

def crear_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=10
            )
        except:
            time.sleep(3)

consumer_queries = crear_consumer("queries", "cache-queries-group")
consumer_responses = crear_consumer("responses", "cache-responses-group")
consumer_dlq = crear_consumer("cache_dlq", "cache-dlq-group")

producer = crear_producer()

print("KAFKA OK", flush=True)


lru = OrderedDict()
lock_lru = threading.Lock()

pendientes = {}
lock_pending = threading.Lock()


def enviar_metricas(tipo, clave=None, retry=0, latencia=0.0):

    global hits, misses, dlq_count, retry_count_global

    evento = {
        "timestamp": time.time(),
        "tipo": tipo,
        "clave": clave,
        "retry_count": retry,
        "latencia": float(latencia),
        "service": "cache"
    }

    try:
        requests.post(URL_METRICAS, json=evento, timeout=1)
    except Exception as e:
        print("ERROR METRICAS:", e, flush=True)

    with lock_metrics:
        if tipo == "hit":
            hits += 1
        elif tipo == "miss":
            misses += 1
        elif tipo == "dlq":
            dlq_count += 1
        elif tipo == "retry":
            retry_count_global += 1


def construir_clave(q):
    op = q.get("operacion")
    zona = q.get("zona_id")
    conf = q.get("confidence_min", 0.5)
    bins = q.get("bins", 5)

    if op == "Q1":
        return f"count:{zona}:conf={conf}"
    if op == "Q2":
        return f"area:{zona}:conf={conf}"
    if op == "Q3":
        return f"density:{zona}:conf={conf}"
    if op == "Q5":
        return f"dist:{zona}:bins={bins}"
    return None


def touch_cache(k):
    with lock_lru:
        lru.pop(k, None)
        lru[k] = time.time()

def evict_if_needed():
    with lock_lru:
        while len(lru) > TAMANO:
            old, _ = lru.popitem(last=False)
            redis_cli.delete(old)
            print("EVICTION ->", old, flush=True)
            enviar_metricas("eviction", old)


def worker_queries():
    print("WORKER QUERIES LISTO", flush=True)

    while True:
        msg_pack = consumer_queries.poll(timeout_ms=1000)

        for _, msgs in msg_pack.items():
            for msg in msgs:

                q = msg.value
                clave = construir_clave(q)

                print("\nCONSULTA:", clave, flush=True)

                if not clave:
                    enviar_metricas("bad")
                    continue

                if redis_cli.get(clave):
                    print("HIT ->", clave, flush=True)
                    enviar_metricas("hit", clave)
                    touch_cache(clave)
                    continue

                print("MISS ->", clave, flush=True)
                enviar_metricas("miss", clave)

                mensaje = {
                    "key": clave,
                    "request": q,
                    "retry_count": 0,
                    "inicio": time.time()
                }

                with lock_pending:
                    if clave not in pendientes:
                        pendientes[clave] = {
                            "mensaje": mensaje,
                            "ultimo_envio": time.time()
                        }

                producer.send("cache_miss", mensaje)
                producer.flush()

                print("Esperando respuesta del generador...", flush=True)


def worker_timeout():
    print("TIMEOUT WORKER INICIADO", flush=True)

    while True:
        time.sleep(1)

        with lock_pending:
            copia = list(pendientes.items())

        for clave, info in copia:

            if time.time() - info["ultimo_envio"] < TIEMPO_REINTENTO:
                continue

            msg = info["mensaje"]

           
            retry = msg.get("retry_count", 0) + 1
            msg["retry_count"] = retry

            print("RETRY", retry, "/", MAX_RETRIES+1 , "->", clave, flush=True)
            enviar_metricas("retry", clave, retry)

            if retry <= MAX_RETRIES:

                producer.send("cache_miss", msg)
                producer.flush()

                with lock_pending:
                    if clave in pendientes:
                        pendientes[clave]["ultimo_envio"] = time.time()

            else:

                print("DLQ ->", clave, flush=True)

                producer.send("cache_dlq", msg)
                producer.flush()

                enviar_metricas("dlq", clave, retry)

                with lock_pending:
                    pendientes.pop(clave, None)



def worker_respuestas():
    print("WORKER RESPUESTAS LISTO", flush=True)

    while True:
        msg_pack = consumer_responses.poll(timeout_ms=1000)

        for _, msgs in msg_pack.items():
            for msg in msgs:

                d = msg.value
                clave = d.get("key")

                if not clave:
                    continue

                inicio = d.get("inicio", time.time())
                latencia = time.time() - inicio

                print("RESPUESTA ->", clave, flush=True)

                
                redis_cli.setex(
                    clave,
                    TTL,
                    json.dumps(d.get("resultado"))
                )

                
                with lock_pending:
                    retry = pendientes.get(clave, {}).get("mensaje", {}).get("retry_count", 0)

                
                enviar_metricas(
                    "response",
                    clave,
                    retry,
                    latencia
                )

                touch_cache(clave)
                evict_if_needed()

                with lock_pending:
                    pendientes.pop(clave, None)

                print("Guardado en cache:", clave, flush=True)


def worker_dlq():
    print("DLQ WORKER LISTO", flush=True)

    while True:
        msg_pack = consumer_dlq.poll(timeout_ms=1000)

        for _, msgs in msg_pack.items():
            for msg in msgs:

                d = msg.value

                print("\n==============================")
                print("DEAD LETTER QUEUE")
                print("KEY:", d.get("key"))
                print("RETRIES:", d.get("retry_count"))
                print("REQUEST:", json.dumps(d.get("request"), indent=2))
                print("==============================\n", flush=True)


if __name__ == "__main__":

    threading.Thread(target=worker_queries, daemon=True).start()
    threading.Thread(target=worker_timeout, daemon=True).start()
    threading.Thread(target=worker_respuestas, daemon=True).start()
    threading.Thread(target=worker_dlq, daemon=True).start()

    print("CACHE RUNNING FULL OBSERVABILITY", flush=True)

    while True:
        time.sleep(10)