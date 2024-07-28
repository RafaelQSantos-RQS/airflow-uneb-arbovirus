ARG AIRFLOW_VERSION=2.9.3
FROM apache/airflow:slim-${AIRFLOW_VERSION}-python3.12

USER root
# Instala dependências do sistema
RUN apt-get update && apt-get install -y \
    gcc \
    libmysqlclient-dev \
    python3-dev \
    pkg-config \
    default-libmysqlclient-dev

USER airflow
# Copia o arquivo de requisitos para dentro do contêiner
COPY requirements.txt /requirements.txt

# Instala as dependências Python
RUN pip install --no-cache-dir \
    "apache-airflow>=${AIRFLOW_VERSION}" \
    -r /requirements.txt