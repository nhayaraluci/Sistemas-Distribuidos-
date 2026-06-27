import time
import random
import json
import requests
import sys
import uuid

time.sleep(10)

URL_CACHE = "http://servicio-cache:5000/query"

PROB_BASURA = 0.15
INTERVALO = 0.8

ZONAS = ["Z1", "Z2", "Z3", "Z4", "Z5"]

CONSULTAS_FRECUENTES = [
    ("Q1", "Z1", 0.5),
    ("Q1", "Z2", 0.5),
    ("Q2", "Z1", 0.5),
    ("Q3", "Z1", 0.5),
    ("Q3", "Z2", 0.5),
]

OPERACIONES = ["Q1", "Q2", "Q3", "Q4", "Q5"]



def zipf_choice(items):
    pesos = [1 / (i + 1) for i in range(len(items))]
    total = sum(pesos)
    return random.choices(items, weights=[p / total for p in pesos])[0]


#
def generar_basura():
    tipos = ["Q99", "INVALID", ""]
    return {
        "request_id": str(uuid.uuid4()),
        "operacion": random.choice(tipos),
        "zona_id": random.choice(ZONAS)
    }


def generar_consulta():

    # BASURA
    if random.random() < PROB_BASURA:
        return generar_basura()

    # FRECUENTES   --> se cambia para aumentar hit 
    if random.random() < 0.8:
        op, zona, conf = random.choice(CONSULTAS_FRECUENTES)
        return {
            "request_id": str(uuid.uuid4()),
            "operacion": op,
            "zona_id": zona,
            "confidence_min": conf,
            "bbox": {}
        }

    # ZIPF 
    if random.random() < 0.4:
        op = random.choice(OPERACIONES)
        zona = zipf_choice(ZONAS)
    else:
        # 4. UNIFORME (IMPORTANTE PARA TAREA)
        op = random.choice(OPERACIONES)
        zona = random.choice(ZONAS)

    conf = random.choice([0.3, 0.5, 0.7])

    consulta = {
        "request_id": str(uuid.uuid4()),
        "operacion": op,
        "zona_id": zona,
        "confidence_min": conf,
        "bbox": {}
    }

    if op == "Q4":
        otros = [z for z in ZONAS if z != zona]
        consulta["zona_id_a"] = zona
        consulta["zona_id_b"] = random.choice(otros)

    if op == "Q5":
        consulta["bins"] = random.choice([5, 10, 15])

    return consulta



def run(n=20):

    print("== GENERADOR DE TRÁFICO INICIADO ==")

    session = requests.Session()

    for i in range(n):

        consulta = generar_consulta()

        print(f"\n--- QUERY {i+1} ---")
        print(json.dumps(consulta, indent=2))

        try:
            respuesta = session.post(URL_CACHE, json=consulta, timeout=(2, 8))

            print("STATUS:", respuesta.status_code)

            try:
                print(respuesta.json())
            except:
                print("RESPUESTA NO JSON")

        except Exception as e:
            print("ERROR:", str(e))

        time.sleep(INTERVALO)

    print("== FIN ==")
    sys.exit(0)


if __name__ == "__main__":
    run(20)