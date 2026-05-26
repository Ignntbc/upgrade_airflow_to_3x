FROM apache/airflow:3.2.1-python3.12

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

ARG AIRFLOW_VERSION=3.2.1
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Базовый Airflow + Celery + Postgres + FAB через extras, с фиксацией версий
RUN pip install --no-cache-dir \
        "apache-airflow[celery,postgres,fab]==${AIRFLOW_VERSION}" \
        --constraint "${CONSTRAINT_URL}"

# Доп. провайдеры (тоже под constraints)
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt --constraint "${CONSTRAINT_URL}"
