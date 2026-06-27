FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY generador_respuestas.py .

EXPOSE 5001

CMD ["python", "generador_respuestas.py"]