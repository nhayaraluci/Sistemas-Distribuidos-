FROM apache/spark:3.5.1

WORKDIR /app

COPY job_spark.py /app/job_spark.py
COPY requirements.txt /app/requirements.txt

USER root

RUN pip install --no-cache-dir -r requirements.txt

CMD ["spark-submit", "/app/job_spark.py"]