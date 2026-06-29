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

MAX_RETRIES = 3

URL_METRICAS = os.getenv("URL_METRICAS", "http://metricas:5002/registrar")


hits = 0
misses = 0
basura = 0
evicciones = 0
dlq = 0

lock_metricas = threading.Lock()


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

print("WORKERS INICIADOS", flush=True)


lru = OrderedDict()
lock = threading.Lock()


pendientes = {}
lock_pendientes = threading.Lock()

TIEMPO_REINTENTO = 5   

def registrar_evento(tipo):

    global hits, misses, basura, evicciones , dlq
    
    
    config_evento = {
    "evento": "config_init",
    "politica": POLITICA,
    "tamano": TAMANO,
    "ttl": TTL
    }

    try:
        requests.post(URL_METRICAS, json=config_evento, timeout=1)
    except:
            pass

    with lock_metricas:

        if tipo == "hit":
            hits += 1

        elif tipo == "miss":
            misses += 1

        elif tipo == "basura":
            basura += 1

        elif tipo == "eviccion":
            evicciones += 1
            
        elif tipo == "dlq":
            dlq += 1

    try:
        r=requests.post(
            URL_METRICAS,
            json={"evento": tipo},
            timeout=2
        )
        print("Metricas:", r.status_code, flush=True)
    except Exception as e:
        print("ERROR enviando metricas:", e, flush=True)

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


def actualizar_cache(k):
    with lock:
        lru.pop(k, None)
        lru[k] = time.time()


def eliminar_exceso():
    with lock:
        while len(lru) > TAMANO:
            viejo, _ = lru.popitem(last=False)
            redis_cli.delete(viejo)

            registrar_evento("eviccion")
            print(" EVICTION ->", viejo, flush=True)

def worker_queries():
    print("WORKER CONSULTAS INICIADO", flush=True)

    while True:
        msg_pack = consumer_queries.poll(timeout_ms=1000)

        for _, mensajes in msg_pack.items():
            for msg in mensajes:

                q = msg.value
                request_id = q.get("request_id")
                clave = construir_clave(q)

                print("\n CONSULTA ->", clave, flush=True)

                if not clave:
                    registrar_evento("basura")
                    continue

                if redis_cli.get(clave):
                    registrar_evento("hit")
                    actualizar_cache(clave)
                    print(" HIT ->", clave, flush=True)
                else:
                    registrar_evento("miss")
                    print(" MISS ->", clave, flush=True)

                    mensaje = {
                        "request_id": request_id,
                        "key": clave,
                        "request": q,
                        "retry_count": 0,
                        "inicio": time.time()
                    }

                    producer.send("cache_miss", mensaje)
                    producer.flush()
                    
                    

                    with lock_pendientes:
                        pendientes[clave] = {
                            "mensaje": mensaje,
                            "ultimo_envio": time.time()
                        }

                    print("----------------------------------------")
                    print("CACHE MISS")
                    print("Consulta enviada a Kafka (cache_miss)")
                    print("Esperando respuesta del generador...")
                    print("----------------------------------------", flush=True)

          
def worker_timeout():

    print("WORKER TIMEOUT INICIADO", flush=True)

    while True:

        time.sleep(1)

        with lock_pendientes:
            
            copiar = list(pendientes.items())

        for clave, info in copiar:

            if time.time() - info["ultimo_envio"] < TIEMPO_REINTENTO:
                continue

            mensaje = info["mensaje"]

            retry = mensaje.get("retry_count", 0) + 1

            mensaje["retry_count"] = retry

            if retry <= MAX_RETRIES:

                print("----------------------------------------")
                print("TIMEOUT")
                print("No llegó respuesta.")
                print(f"Retry {retry} de {MAX_RETRIES}")
                print("KEY:", clave)
                print("Reenviando consulta...")
                print("----------------------------------------", flush=True)

                producer.send("cache_miss", mensaje)
                producer.flush()

                with lock_pendientes:
                    pendientes[clave]["ultimo_envio"] = time.time()

            else:
                 
                mensaje["retry_count"] = MAX_RETRIES

                producer.send("cache_dlq", mensaje)
                producer.flush()
                
                registrar_evento("dlq")

                print("----------------------------------------")
                print("MAXIMO DE REINTENTOS")
                print("Consulta enviada a cache_dlq")
                print("KEY:", clave)
                print("----------------------------------------", flush=True)

                with lock_pendientes:
                    del pendientes[clave]
   
def worker_respuestas():

    print("WORKER RESPUESTAS INICIADO", flush=True)

    while True:

        msg_pack = consumer_responses.poll(timeout_ms=1000)

        for _, mensajes in msg_pack.items():

            for msg in mensajes:

                d = msg.value

                clave = d.get("key")
                resultado = d.get("resultado")
                inicio = d.get("inicio")

                latencia = time.time() - inicio if inicio else None
                    
                with lock_pendientes:
                    if clave not in pendientes:
                        print("----------------------------------------")
                        print("RESPUESTA TARDIA DESCARTADA")
                        print("KEY:", clave)
                        print("La consulta ya habia sido enviada a DLQ")
                        print("----------------------------------------")
                        continue

                if not clave:
                    continue

                redis_cli.setex(
                    clave,
                    TTL,
                    json.dumps(resultado)
                )
                try:
                    r=requests.post(
                        URL_METRICAS,
                        json={
                            "evento": "respuesta",
                            "latencia": latencia,
                            "retry_count": d.get("retry_count", 0)
                        },
                        timeout=2
                    )
                    print("Metricas:", r.status_code, flush=True)
                except Exception as e:
                    print("ERROR enviando metricas:", e, flush=True)

                actualizar_cache(clave)
                eliminar_exceso()

                with lock_pendientes:
                    if clave in pendientes:
                        del pendientes[clave]

                print("----------------------------------------")
                print("RESPUESTA RECIBIDA")
                print("KEY:", clave)
                print("Resultado almacenado en Redis")
                print("Consulta eliminada de pendientes")
                print("----------------------------------------", flush=True)
                
                             

def worker_dlq():

    print("WORKER DLQ INICIADO", flush=True)

    while True:

        msg_pack = consumer_dlq.poll(timeout_ms=1000)

        for _, mensajes in msg_pack.items():

            for msg in mensajes:

                d = msg.value

                print("")
                print("========================================")
                print("DEAD LETTER QUEUE")
                print("========================================")
                print("KEY:", d["key"])
                print("REINTENTOS:", d["retry_count"])
                print("CONSULTA ORIGINAL:")
                print(json.dumps(d["request"], indent=4))
                print("========================================")
                print("")
                

if __name__ == "__main__":
    threading.Thread(target=worker_queries, daemon=True).start()
    threading.Thread(target=worker_respuestas, daemon=True).start()
    threading.Thread(target=worker_timeout, daemon=True).start()
    threading.Thread(target=worker_dlq, daemon=True).start()

    print("CACHE RUNNING ", flush=True)

    while True:
        time.sleep(10)