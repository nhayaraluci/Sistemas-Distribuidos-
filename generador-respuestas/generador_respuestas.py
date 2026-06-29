import time
import random
import json
import pandas as pd
import os
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

print("INICIANDO GENERADOR DE RESPUESTAS", flush=True)

def crear_dataset():
    zonas = ["Z1", "Z2", "Z3", "Z4", "Z5"]

    datos = []
    for i in range(2000):
        datos.append({
            "id": i,
            "zona": random.choice(zonas),
            "area": random.uniform(10, 500),
            "confianza": random.random()
        })

    return pd.DataFrame(datos)


df = crear_dataset()

AREAS_ZONA = {
    "Z1": 0.625,
    "Z2": 0.75,
    "Z3": 1.2,
    "Z4": 0.9,
    "Z5": 1.1
}


def crear_consumidor():
    return KafkaConsumer(
        "cache_miss",
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="generador-respuestas"
    )


def crear_productor():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all"
    )


consumidor = crear_consumidor()
productor = crear_productor()


def filtrar_datos(zona, confianza_minima):
    return df[
        (df["zona"] == zona) &
        (df["confianza"] >= confianza_minima)
    ]


def procesar_consulta(consulta):
    request = consulta["request"]
    operacion = request["operacion"]

    zona = request.get("zona_id")
    confianza = float(request.get("confidence_min", 0))

    time.sleep(random.uniform(0.2, 0.8))

    if operacion == "Q1":
        datos = filtrar_datos(zona, confianza)
        return {"conteo": len(datos)}

    elif operacion == "Q2":
        datos = filtrar_datos(zona, confianza)
        return {
            "promedio_area": float(datos["area"].mean()) if len(datos) else 0,
            "total_area": float(datos["area"].sum()) if len(datos) else 0
        }

    elif operacion == "Q3":
        datos = filtrar_datos(zona, confianza)
        area = AREAS_ZONA.get(zona, 1)
        return {"densidad": len(datos) / area}

    elif operacion == "Q4":
        zona_a = request.get("zona_id_a")
        zona_b = request.get("zona_id_b")

        datos_a = filtrar_datos(zona_a, confianza)
        datos_b = filtrar_datos(zona_b, confianza)

        dens_a = len(datos_a) / AREAS_ZONA.get(zona_a, 1)
        dens_b = len(datos_b) / AREAS_ZONA.get(zona_b, 1)

        return {
            "zona_a": dens_a,
            "zona_b": dens_b,
            "ganador": zona_a if dens_a > dens_b else zona_b
        }

    elif operacion == "Q5":
        datos = filtrar_datos(zona, confianza)
        bins = int(request.get("bins", 5))

        if len(datos) == 0:
            return {"bins": bins, "histograma": []}

        cortes = pd.cut(datos["confianza"], bins=bins)
        frecuencias = cortes.value_counts().sort_index()

        return {
            "bins": bins,
            "histograma": [
                {"intervalo": str(k), "frecuencia": int(v)}
                for k, v in frecuencias.items()
            ]
        }

    return {}


print("ESCUCHANDO cache_miss", flush=True)

for mensaje in consumidor:

    datos = mensaje.value

    print("MISS RECIBIDO")
    print(json.dumps(datos, indent=2, ensure_ascii=False), flush=True)

    resultado = procesar_consulta(datos)

    respuesta = {
    "key": datos["key"],
    "resultado": resultado,
    "inicio": datos["inicio"],
    "retry_count": datos.get("retry_count", 0)
    }

    productor.send("responses", respuesta)
    productor.flush()

    print("RESPUESTA ENVIADA A KAFKA: responses", flush=True)