FROM apache/airflow:3.0.6

USER airflow


#pinning Airflow to 3.0.6 to prevent pip from inadvertently upgrading or downgrading the core installation.
RUN pip install --no-cache-dir \
    "apache-airflow==3.0.6" \    
    "apache-airflow-providers-fab==2.3.0" \
    "apache-airflow-providers-amazon==9.9.0" \
    "apache-airflow-providers-snowflake==6.4.0" \
    "astronomer-cosmos==1.10.3"

RUN python -m venv /home/airflow/dbt_venv

RUN /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
    dbt-core \
    dbt-snowflake