import time
import random
import pandas as pd
import numpy as np
from flask import Flask, request, jsonify

app = Flask(__name__)


def generar_dataset():
    zonas = ["Z1", "Z2", "Z3", "Z4", "Z5"]

    datos = []

    for i in range(2000):
        datos.append({
            "id": i,
            "zona_id": random.choice(zonas),
            "area_in_meters": random.uniform(10, 500),
            "confidence": random.uniform(0, 1)
        })

    return pd.DataFrame(datos)


df = generar_dataset()
print("Dataset cargado en memoria")



AREAS_ZONA = {
    "Z1": 0.625,
    "Z2": 0.75,
    "Z3": 1.2,
    "Z4": 0.9,
    "Z5": 1.1
}



def filtrar_datos(zona, conf_min):
    return df[
        (df["zona_id"] == zona) &
        (df["confidence"] >= conf_min)
    ]



@app.route("/compute", methods=["POST"])
def compute():

    inicio = time.time()
    consulta = request.json

    operacion = consulta.get("operacion")
    zona = consulta.get("zona_id")
    conf = float(consulta.get("confidence_min", 0.0))

    # simulación de carga computacional
    time.sleep(random.uniform(0.2, 1.0))

    resultado = {}

    
    if operacion == "Q1":
        datos = filtrar_datos(zona, conf)

        resultado = {
            "conteo_puntos": len(datos)
        }

    
    elif operacion == "Q2":
        datos = filtrar_datos(zona, conf)

        resultado = {
            "avg_area": float(datos["area_in_meters"].mean()) if len(datos) else 0,
            "total_area": float(datos["area_in_meters"].sum()) if len(datos) else 0,
            "n": int(len(datos))
        }

    
    elif operacion == "Q3":
        datos = filtrar_datos(zona, conf)

        count = len(datos)
        area_km2 = AREAS_ZONA.get(zona, 1)

        resultado = {
            "densidad_puntos_km2": count / area_km2
        }

    
    elif operacion == "Q4":

        zona_a = consulta.get("zona_id_a")
        zona_b = consulta.get("zona_id_b")

        datos_a = filtrar_datos(zona_a, conf)
        datos_b = filtrar_datos(zona_b, conf)

        dens_a = len(datos_a) / AREAS_ZONA.get(zona_a, 1)
        dens_b = len(datos_b) / AREAS_ZONA.get(zona_b, 1)

        resultado = {
            "zone_a_density": dens_a,
            "zone_b_density": dens_b,
            "winner": zona_a if dens_a > dens_b else zona_b
        }

        
    elif operacion == "Q5":

        datos = filtrar_datos(zona, conf)
        bins = int(consulta.get("bins", 5))

        if len(datos) == 0:
            resultado = {
                "bins": bins,
                "histograma": [],
                "n": 0
            }

        else:

            conteo, limites = pd.cut(
                datos["confidence"],
                bins=bins,
                retbins=True
            )

            frecuencias = conteo.value_counts().sort_index()

            resultado = {
                "bins": bins,
                "histograma": [
                    {
                        "intervalo": str(intervalo),
                        "frecuencia": int(frecuencia)
                    }
                    for intervalo, frecuencia in frecuencias.items()
                ],
                "n": int(len(datos))
            }

    
    else:
        return jsonify({
            "estado": "error",
            "msg": "operacion invalida"
        }), 400

    
    latencia = time.time() - inicio

    return jsonify({
        "resultado": resultado,
        "latencia_seg": round(latencia, 4)
    }), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)