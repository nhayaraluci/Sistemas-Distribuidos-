import os
import json
import time
import redis
import requests
import numpy as np
from flask import Flask, request, jsonify
from collections import OrderedDict

app = Flask(__name__)


#FIFO :Saca la ms antigua.
#LRU : Saca la menos usada recientemente.

politica = os.getenv("POLITICA", "LRU")
tamano_cache = int(os.getenv("TAMANO", 5))
ttl_segundos = int(os.getenv("TTL", 60))

redis_host = os.getenv("REDIS_HOST", "redis")
redis_port = int(os.getenv("REDIS_PORT", 6379))

url_respuestas = "http://contenedor-respuestas:5001/compute"
url_metricas = "http://contenedor-metricas:5002/registrar"

redis_cliente = redis.Redis(
    host=redis_host,
    port=redis_port,
    decode_responses=True
)

redis_cliente.ping()

# ---------------- ESTRUCTURAS ----------------
fifo = []
lru = OrderedDict()
latencias = []


# ---------------- MÉTRICAS ----------------
def registrar_evento(tipo, llave=None, latencia=None):

    evento = {
        "evento": tipo,
        "llave": llave,
        "latencia": latencia,
        "politica": politica,
        "ttl": ttl_segundos,
        "tamano": tamano_cache
    }

    if latencia is not None:
        latencias.append(latencia)

    # peso cache
    if llave:
        try:
            val = redis_cliente.get(llave)
            if val:
                evento["peso_bytes"] = len(val.encode("utf-8"))
        except:
            evento["peso_bytes"] = 0

    try:
        requests.post(url_metricas, json=evento, timeout=1)
    except:
        pass


# ---------------- KEY BUILDER ----------------
def construir_llave(q):

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
        a = q.get("zona_id_a")
        b = q.get("zona_id_b")
        if a and b:
            a, b = sorted([a, b])
        return f"compare:{a}:{b}:conf={conf}"

    if op == "Q5":
        return f"dist:{zona}:bins={bins}"

    return None


# ---------------- VALIDACIÓN ----------------
def validar_consulta(q):
    op = q.get("operacion")

    if op not in ["Q1", "Q2", "Q3", "Q4", "Q5"]:
        return False

    if op in ["Q1", "Q2", "Q3", "Q5"] and not q.get("zona_id"):
        return False

    if op == "Q4":
        return bool(q.get("zona_id_a") and q.get("zona_id_b"))

    return True


# ---------------- CACHE CONTROL ----------------
def actualizar_indice(llave):
    if politica == "LRU":
        if llave in lru:
            lru.pop(llave)
        lru[llave] = time.time()
    else:
        if llave in fifo:
            fifo.remove(llave)
        fifo.append(llave)


def expulsar():
    estructura = fifo if politica == "FIFO" else lru

    while len(estructura) > tamano_cache:
        if politica == "FIFO":
            vieja = fifo.pop(0)
        else:
            vieja, _ = lru.popitem(last=False)

        redis_cliente.delete(vieja)
        registrar_evento("eviccion", llave=vieja)


# ---------------- QUERY ----------------
@app.route("/query", methods=["POST"])
def query():

    inicio = time.time()
    q = request.json

    if not validar_consulta(q):
        registrar_evento("basura")
        return jsonify({"estado": "error"}), 400

    llave = construir_llave(q)

    if not llave:
        registrar_evento("basura")
        return jsonify({"estado": "error"}), 400

    # ---------------- HIT ----------------
    cache = redis_cliente.get(llave)

    if cache:
        actualizar_indice(llave)
        registrar_evento("hit", llave, time.time() - inicio)

        return jsonify({
            "origen": "cache",
            "resultado": json.loads(cache)
        }), 200

    # ---------------- MISS ----------------
    try:
        resp = requests.post(url_respuestas, json=q, timeout=(2, 6))

        if resp.status_code != 200:
            registrar_evento("error", llave, time.time() - inicio)
            return jsonify({"estado": "error"}), 500

        body = resp.json()

        data = body.get("resultado", {})

        redis_cliente.setex(llave, ttl_segundos, json.dumps(data))

        actualizar_indice(llave)
        expulsar()

        registrar_evento("miss", llave, time.time() - inicio)

        return jsonify({
            "origen": "generador-respuesta",
            "resultado": data
        }), 200

    except:
        registrar_evento("error", llave, time.time() - inicio)
        return jsonify({"estado": "caido"}), 200


# ---------------- LATENCIAS ----------------
@app.route("/latencias", methods=["GET"])
def latencias_endpoint():

    if not latencias:
        return jsonify({"mensaje": "sin datos"}), 200

    return jsonify({
        "p50": float(np.percentile(latencias, 50)),
        "p90": float(np.percentile(latencias, 90)),
        "p95": float(np.percentile(latencias, 95)),
        "promedio": float(np.mean(latencias))
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)