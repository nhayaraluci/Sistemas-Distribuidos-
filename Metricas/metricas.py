import os
import time
import redis
import numpy as np
from flask import Flask, request, jsonify

app = Flask(__name__)

EVENTOS = []

CONFIG = {
    "politica": None,
    "tamano": None,
    "ttl": None
}

redis_cliente = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True
)

URL_CACHE = os.getenv("URL_CACHE", "http://contenedor-cache:5000/registrar")



def normalizar_evento(evento: dict):

    tipo = evento.get("evento")

    
    if tipo in ["eviccion", "eviction"]:
        evento["evento"] = "eviction"

    return evento



@app.route("/registrar", methods=["POST"])
def registrar():

    evento = request.json or {}
    evento["timestamp"] = time.time()

    evento = normalizar_evento(evento)

    
    if evento.get("politica") is not None:
        CONFIG["politica"] = evento["politica"]

    if evento.get("tamano") is not None:
        CONFIG["tamano"] = evento["tamano"]

    if evento.get("ttl") is not None:
        CONFIG["ttl"] = evento["ttl"]

    EVENTOS.append(evento)

    return jsonify({"ok": True}), 200

@app.route("/reporte", methods=["GET"])
def reporte():

    if not EVENTOS:
        return jsonify({"mensaje": "sin datos"}), 200

    
    hits = sum(1 for e in EVENTOS if e.get("evento") == "hit")
    misses = sum(1 for e in EVENTOS if e.get("evento") == "miss")
    basura = sum(1 for e in EVENTOS if e.get("evento") == "basura")
    evicciones = sum(1 for e in EVENTOS if e.get("evento") == "eviction")

    respuestas = sum(1 for e in EVENTOS if e.get("evento") == "respuesta")

    
    latencias = [
        e["latencia"]
        for e in EVENTOS
        if isinstance(e.get("latencia"), (int, float))
    ]

    if latencias:
        promedio = float(np.mean(latencias))
        p50 = float(np.percentile(latencias, 50))
        p90 = float(np.percentile(latencias, 90))
        p95 = float(np.percentile(latencias, 95))
    else:
        promedio = p50 = p90 = p95 = 0.0

   
    tiempos = [e["timestamp"] for e in EVENTOS if "timestamp" in e]

    if len(tiempos) > 1:
        dt = max(tiempos) - min(tiempos)
        throughput = round(len(EVENTOS) / dt, 2) if dt > 0 else 0
    else:
        throughput = 0

    
    total = hits + misses
    hit_rate = round((hits / total) * 100, 2) if total else 0

    
    peso_total = 0
    peso_por_llave = {}

    try:
        for k in redis_cliente.keys("*"):
            v = redis_cliente.get(k)
            if v:
                tam = len(k.encode("utf-8")) + len(v.encode("utf-8"))
                peso_por_llave[k] = tam
                peso_total += tam
    except:
        pass

    return jsonify({
        "configuracion_sistema": CONFIG,

        "resumen_trafico": {
            "cache_hits": hits,
            "cache_misses": misses,
            "consultas_basura": basura,
            "total ": hits+misses+basura,
            "hit_rate": f"{hit_rate}%"
        },

        "metricas_rendimiento": {
            "latencia_promedio_seg": round(promedio, 4),
            "p50": p50,
            "p90": p90,
            "p95": p95,
            "throughput_eventos_por_seg": throughput,
            "evicciones": evicciones
        },

        "analisis_espacio_redis": {
            "peso_total_bytes": peso_total,
            "peso_por_llave": peso_por_llave,
            "llaves_en_cache": len(peso_por_llave)
        }
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002)