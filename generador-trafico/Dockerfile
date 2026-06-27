
FROM python:3.10-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY generador_trafico.py .


CMD ["python", "traffic_generator.py"]