FROM apache/airflow:3.1.0
COPY requirements.txt .
RUN pip install -r requirements.txt


USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    build-essential gcc g++ \
    unzip curl \
 && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/airflow/data
COPY data/ /opt/airflow/data/

RUN chown -R airflow:root /opt/airflow/data
USER airflow