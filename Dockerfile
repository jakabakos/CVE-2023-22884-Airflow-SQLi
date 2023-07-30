FROM apache/airflow:2.5.0

RUN pip install apache-airflow-providers-mysql==3.4.0
