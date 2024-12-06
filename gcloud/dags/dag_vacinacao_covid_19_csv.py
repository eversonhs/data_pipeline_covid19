
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'vacinacao_covid19_csv',
    default_args=default_args,
    description='First execution of COVID-19 vacination pipeline',
    start_date=datetime(2024,11, 6),
    tags=['vacinacao_covid19'],
    schedule=None,
    catchup=False
) as dag:
    
    exec_date = datetime(2023, 11, 6)

    bronze_silver_task = DataprocCreateBatchOperator(
        task_id=f"create_job_bronze_silver_csv",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://pgii-dataproc/silver/bronze_silver_vacinacao_covid_19.py",
                "args": ["--output_date", exec_date.strftime("%Y-%m-%d")]
            },
            "environment_config": {
                "execution_config": {
                    "ttl": "7200s",
                },
            },
        },
        region="us-central1",
        batch_id=f'bronze-silver-vacinacao-covid19-csv-{exec_date.strftime("%Y%m%d%H%M%S")}',
    )
    
    silver_gold_dm_campanhas = DataprocCreateBatchOperator(
        task_id=f"create_job_silver_gold_dm_campanhas",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://pgii-dataproc/gold/silver_gold_dm_campanhas.py",
                "args": ["--date", exec_date.strftime("%Y-%m-%d")]
            },
            "runtime_config": {
                "properties": {
                    "spark.jars.packages": "io.delta:delta-spark_2.13:3.2.1"
                }
            },
            "environment_config": {
                "execution_config": {
                    "ttl": "7200s",
                },
            },
        },
        region="us-central1",
        batch_id=f'silver-gold-dm-campanhas-{exec_date.strftime("%Y%m%d%H%M%S")}',
    )
    
    silver_gold_dm_estabelecimentos = DataprocCreateBatchOperator(
        task_id=f"create_job_silver_gold_dm_estabelecimentos",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://pgii-dataproc/gold/silver_gold_dm_estabelecimentos.py",
                "args": ["--date", exec_date.strftime("%Y-%m-%d")]
            },
            "runtime_config": {
                "properties": {
                    "spark.jars.packages": "io.delta:delta-spark_2.13:3.2.1"
                }
            },
            "environment_config": {
                "execution_config": {
                    "ttl": "7200s",
                },
            },
        },
        region="us-central1",
        batch_id=f'silver-gold-dm-estabelecimentos-{exec_date.strftime("%Y%m%d%H%M%S")}',
    )
    
    silver_gold_dm_vacinas = DataprocCreateBatchOperator(
        task_id=f"create_job_silver_gold_dm_vacinas",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://pgii-dataproc/gold/silver_gold_dm_vacinas.py",
                "args": ["--date", exec_date.strftime("%Y-%m-%d")]
            },
            "runtime_config": {
                "properties": {
                    "spark.jars.packages": "io.delta:delta-spark_2.13:3.2.1"
                }
            },
            "environment_config": {
                "execution_config": {
                    "ttl": "7200s",
                },
            },
        },
        region="us-central1",
        batch_id=f'silver-gold-dm-vacinas-{exec_date.strftime("%Y%m%d%H%M%S")}',
    )
    
    silver_gold_dm_municipios = DataprocCreateBatchOperator(
        task_id=f"create_job_silver_gold_dm_municipios",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://pgii-dataproc/gold/silver_gold_dm_municipios.py",
                "args": ["--date", exec_date.strftime("%Y-%m-%d")]
            },
            "runtime_config": {
                "properties": {
                    "spark.jars.packages": "io.delta:delta-spark_2.13:3.2.1"
                }
            },
            "environment_config": {
                "execution_config": {
                    "ttl": "7200s",
                },
            },
        },
        region="us-central1",
        batch_id=f'silver-gold-dm-municipios-{exec_date.strftime("%Y%m%d%H%M%S")}',
    )

    silver_gold_dm_pacientes = DataprocCreateBatchOperator(
        task_id=f"create_job_silver_gold_dm_pacientes",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://pgii-dataproc/gold/silver_gold_dm_pacientes.py",
                "args": ["--date", exec_date.strftime("%Y-%m-%d")]
            },
            "runtime_config": {
                "properties": {
                    "spark.jars.packages": "io.delta:delta-spark_2.13:3.2.1"
                }
            },
            "environment_config": {
                "execution_config": {
                    "ttl": "7200s",
                },
            },
        },
        region="us-central1",
        batch_id=f'silver-gold-dm-pacientes-{exec_date.strftime("%Y%m%d%H%M%S")}',
    )

    silver_gold_ft_vacinacao = DataprocCreateBatchOperator(
        task_id=f"create_job_silver_gold_ft_vacinacao",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://pgii-dataproc/gold/silver_gold_ft_vacinacao.py",
                "args": ["--date", exec_date.strftime("%Y-%m-%d")]
            },
            "runtime_config": {
                "properties": {
                    "spark.jars.packages": "io.delta:delta-spark_2.13:3.2.1"
                }
            },
            "environment_config": {
                "execution_config": {
                    "ttl": "7200s",
                },
            },
        },
        region="us-central1",
        batch_id=f'silver-gold-ft-vacinacao-{exec_date.strftime("%Y%m%d%H%M%S")}',
    )
   
    bronze_silver_task >>  \
    silver_gold_dm_campanhas >> silver_gold_dm_estabelecimentos >> \
    silver_gold_dm_vacinas >> silver_gold_dm_municipios >> \
    silver_gold_dm_pacientes >> silver_gold_ft_vacinacao
    
    