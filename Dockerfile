FROM apache/airflow:2.10.5

USER root

# Настройка зеркал
RUN echo "[global]" > /etc/pip.conf && \
    echo "index-url = https://pypi.tuna.tsinghua.edu.cn/simple" >> /etc/pip.conf && \
    echo "timeout = 120" >> /etc/pip.conf

USER airflow

# Установка через constraints-файл
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip install --upgrade pip

RUN pip install --no-cache-dir \
    --timeout 120 \
    --constraint "${CONSTRAINT_URL}" \
    apache-airflow-providers-amazon==9.2.0