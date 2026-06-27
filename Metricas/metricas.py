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



redis_host = os.getenv("REDIS_HOST", "redis")
redis_port = int(os.getenv("REDIS_PORT", 6379))

redis_cliente = redis.Redis(
    host=redis_host,
    port=redis_port,
    decode_responses=True
)



@app.route("/registrar", methods=["POST"])
def registrar():

    evento = request.json
    evento["timestamp"] = time.time()

    CONFIG["politica"] = evento.get("politica")
    CONFIG["tamano"] = evento.get("tamano")
    CONFIG["ttl"] = evento.get("ttl")

    EVENTOS.append(evento)

    return jsonify({"ok": True}), 200




@app.route("/reporte", methods=["GET"])
def reporte():

    if len(EVENTOS) == 0:
        return jsonify({"mensaje": "sin datos"}), 200

    

    hits = sum(1 for e in EVENTOS if e["evento"] == "hit")
    misses = sum(1 for e in EVENTOS if e["evento"] == "miss")
    basura = sum(1 for e in EVENTOS if e["evento"] == "basura")
    evicciones = sum(1 for e in EVENTOS if e["evento"] == "eviccion")

    

    latencias = [
        e["latencia"]
        for e in EVENTOS
        if e.get("latencia") is not None
    ]

    if latencias:
        promedio = float(np.mean(latencias))
        p50 = float(np.percentile(latencias, 50))
        p90 = float(np.percentile(latencias, 90))
        p95 = float(np.percentile(latencias, 95))
    else:
        promedio = p50 = p90 = p95 = 0.0

    

    tiempos = [e["timestamp"] for e in EVENTOS]

    if len(tiempos) > 1:
        tiempo_total = max(tiempos) - min(tiempos)

        if tiempo_total == 0:
            throughput = 0
        else:
            throughput = round(len(EVENTOS) / tiempo_total, 2)
    else:
        throughput = 0

    

    consultas_validas = hits + misses

    if consultas_validas > 0:
        hit_rate = round((hits / consultas_validas) * 100, 2)
    else:
        hit_rate = 0

    

    peso_total = 0
    peso_por_llave = {}

    try:

        claves = redis_cliente.keys("*")

        for llave in claves:

            valor = redis_cliente.get(llave)

            if valor is None:
                continue

            # tamaño aproximado: llave + valor
            tam = len(llave.encode("utf-8")) + len(valor.encode("utf-8"))

            peso_por_llave[llave] = tam
            peso_total += tam

    except Exception:

        peso_total = 0
        peso_por_llave = {}

    

    return jsonify({

        "configuracion_sistema": CONFIG,

        "resumen_trafico": {
            "total_eventos": consultas_validas + basura,
            "cache_hits": hits,
            "cache_misses": misses,
            "consultas_basura": basura,
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

    }), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002)