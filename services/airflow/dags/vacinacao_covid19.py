
from datetime import timedelta, datetime, UTC
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'vacinacao_covid19_daily_ingestion',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime.now(),
    tags=['vacinacao_covid19'],
) as dag:

    now = datetime.now(UTC)
    ingestion_date = now - timedelta(days=1)
    ingestion_task = BashOperator(
        task_id='ingest_vacinacao_covid19',
        bash_command=f'python {os.environ["JOBS_PATH"]}/raw_vacinacao_covid19_json.py --date={ingestion_date.strftime("%Y-%m-%d")}'
    )

    rw_tr_task = SparkSubmitOperator(
        task_id='raw_to_trusted_vacinacao_covid_19_json',
        name='rw_tr_vacinacao_covid_19_json', 
        application=f'{os.environ["JOBS_PATH"]}/incremental/rw_tr_vacinacao_covid_19_json.py',
        conn_id='spark_cluster',
        jars=os.environ["POSTGRESQL_JDBC_DRIVER"],
        driver_class_path=os.environ["POSTGRESQL_JDBC_DRIVER"],
        executor_memory='8G',
        executor_cores=4
    )

    dimensions = {
        'dm_pacientes': 'tr_rf_dm_pacientes.py',
        'dm_campanhas': 'tr_rf_dm_campanhas.py',
        'dm_estabelecimentos': 'tr_rf_dm_estabelecimentos.py',
        'dm_municipios': 'tr_rf_dm_municipios.py',
        'dm_vacinas': 'tr_rf_dm_vacinas.py'
    }

    dm_tasks = []
    for dm, script in dimensions.items():
        dm_tasks.append(
            SparkSubmitOperator(
                task_id=dm,
                name=dm, 
                application=f'{os.environ["JOBS_PATH"]}/incremental/{script}',
                conn_id='spark_cluster',
                jars=os.environ["POSTGRESQL_JDBC_DRIVER"],
                driver_class_path=os.environ["POSTGRESQL_JDBC_DRIVER"],
                executor_memory='8G',
                executor_cores=4
            )
        )

    ft_task = SparkSubmitOperator(
        task_id='ft_vacinacao',
        name='ft_vacinacao',
        application=f'{os.environ["JOBS_PATH"]}/incremental/tr_rf_ft_vacinacao.py',
        conn_id='spark_cluster',
        jars=os.environ["POSTGRESQL_JDBC_DRIVER"],
        driver_class_path=os.environ["POSTGRESQL_JDBC_DRIVER"],
        executor_memory='8G',
        executor_cores=4
    )
   
    ingestion_task >> rw_tr_task >> dm_tasks >> ft_task