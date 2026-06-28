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

URL_METRICAS = os.getenv("URL_METRICAS", "http://contenedor-metricas:5002/registrar")

hits = 0
misses = 0
basura = 0
evicciones = 0

latencias = []
tiempos = []

lock_metricas = threading.Lock()


redis_cli = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True
)

print("CONECTANDO REDIS...........", flush=True)

while True:
    try:
        redis_cli.ping()
        break
    except:
        time.sleep(2)

print("REDIS OK", flush=True)


def enviar_metricas(evento):
    try:
        requests.post(URL_METRICAS, json=evento, timeout=1)
    except:
        pass



def crear_consumer(topico, grupo):
    while True:
        try:
            return KafkaConsumer(
                topico,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id=grupo
            )
        except Exception as e:
            print("ERROR CONSUMER...:", e, flush=True)
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
        except Exception as e:
            print("ERROR PRODUCER....-.:", e, flush=True)
            time.sleep(3)


consumer_queries = crear_consumer("queries", "cache-queries-group")
consumer_responses = crear_consumer("responses", "cache-responses-group")
producer = crear_producer()

print("WORKER.. INICIADO..", flush=True)


lru = OrderedDict()
lock = threading.Lock()



def normalizar(k):
    return str(k).strip().lower()



def registrar_evento(tipo, request_id=None, clave=None, latencia=None):
    global hits, misses, basura, evicciones

    evento = {
        "evento": tipo,
        "request_id": request_id,
        "clave": clave,
        "latencia": latencia,
        "timestamp": time.time(),
        "politica": POLITICA,
        "tamano": TAMANO,
        "ttl": TTL
    }

    with lock_metricas:
        if tipo == "hit":
            hits += 1
        elif tipo == "miss":
            misses += 1
        elif tipo == "basura":
            basura += 1
        elif tipo == "eviccion":
            evicciones += 1

        if latencia is not None:
            latencias.append(latencia)

        tiempos.append(time.time())

    enviar_metricas(evento)



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
    if op == "Q4":
        a, b = sorted([q.get("zona_id_a"), q.get("zona_id_b")])
        return f"compare:{a}:{b}:conf={conf}"
    if op == "Q5":
        return f"dist:{zona}:bins={bins}"

    return None



def actualizar_cache(k):
    with lock:
        k = normalizar(k)
        lru.pop(k, None)
        lru[k] = time.time()


def eliminar_exceso():
    global evicciones

    with lock:
        while len(lru) > TAMANO:
            viejo, _ = lru.popitem(last=False)

            redis_cli.delete(viejo)

            evicciones += 1
            registrar_evento("eviccion", clave=viejo)

            print("EVICTION : ", viejo, flush=True)



def worker_queries():
    print("WORKER CONSULTAS INICIADO", flush=True)

    while True:
        msg_pack = consumer_queries.poll(timeout_ms=1000)

        if not msg_pack:
            continue

        for _, mensajes in msg_pack.items():
            for msg in mensajes:

                inicio = time.time()

                q = msg.value
                request_id = q.get("request_id")

                clave = construir_clave(q)
                if clave:
                    clave = normalizar(clave)

                print("\nCONSULTA RECIBIDA", flush=True)
                print(json.dumps(q, indent=2), flush=True)
                print("CLAVE:", clave, flush=True)

                if not clave:
                    registrar_evento("basura", request_id=request_id)
                    continue

                valor = redis_cli.get(clave)

                
                if valor is not None:
                    lat = time.time() - inicio
                    registrar_evento("hit", request_id, clave, lat)
                    actualizar_cache(clave)

                    print("RESULTADO: HIT ->", clave, flush=True)

                
                else:
                    lat = time.time() - inicio
                    registrar_evento("miss", request_id, clave, lat)

                    print("RESULTADO: MISS ->", clave, flush=True)

                    producer.send("cache_miss", {
                        "request_id": request_id,
                        "key": clave,
                        "request": q
                    })
                    producer.flush()



def worker_respuestas():
    print("WORKER RESPUESTAS INICIADO", flush=True)

    while True:
        msg_pack = consumer_responses.poll(timeout_ms=1000)

        if not msg_pack:
            continue

        for _, mensajes in msg_pack.items():
            for msg in mensajes:

                d = msg.value

                clave = d.get("key")
                resultado = d.get("resultado")

                if not clave or resultado is None:
                    continue

                clave = normalizar(clave)

                redis_cli.setex(clave, TTL, json.dumps(resultado))

                actualizar_cache(clave)
                eliminar_exceso()

                registrar_evento("respuesta", clave=clave)

                print("CACHE ACTUALIZADO ->", clave, flush=True)


if __name__ == "__main__":
    threading.Thread(target=worker_queries, daemon=True).start()
    threading.Thread(target=worker_respuestas, daemon=True).start()

    print("CACHE CORRIENDOO", flush=True)

    while True:
        time.sleep(10)