FROM apache/airflow:2.9.3

COPY requirements-airflow.txt /requirements-airflow.txt

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt" \
    -r /requirements-airflow.txt