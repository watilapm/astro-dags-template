FROM quay.io/astronomer/astro-runtime:12.4.0

ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
ENV AIRFLOW__ASTRO_SDK__DATAFRAME_ALLOW_UNSAFE_STORAGE=True
ENV AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=30
ENV AIRFLOW__CORE__DAGS_FOLDER=~/airflow/dags
RUN apt-get update && apt-get install -y build-essential gcc python3-dev
RUN python -m pip install --upgrade pip



