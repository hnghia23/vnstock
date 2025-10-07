FROM apache/airflow:2.10.5

USER airflow

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

