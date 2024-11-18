
from datetime import timedelta, datetime, UTC
from airflow import DAG
from airflow.operators.bash import BashOperator
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
    'vacinacao_covid19',
    default_args=default_args,
    description='Daily update of COVID19 vacination status',
    schedule_interval=timedelta(days=1),
    start_date=datetime.now(),
    tags=['vacinacao_covid19'],
) as dag:
    
    now = datetime.now(UTC)
    ingestion_date = now - timedelta(days=2)
    
    ingestion_task = BashOperator(
        task_id='ingest_vacinacao_covid19',
        bash_command=f'''gcloud storage cp --recursive gs://pgii-composer/jobs ~/ &&
        python ~/jobs/bronze/bronze_vacinacao_covid_19_json.py --date={ingestion_date.strftime("%Y-%m-%d")}'''
    )

    bronze_silver_task = DataprocCreateBatchOperator(
        task_id=f"create_job_bronze_silver",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://pgii-dataproc/silver/bronze_silver_vacinacao_covid_19_json.py",
                "args": ["--year", str(ingestion_date.year), "--month", str(ingestion_date.month), "--day", str(ingestion_date.day)]
            },
            "environment_config": {
                "execution_config": {
                    "ttl": "7200s",
                },
            },
        },
        region="us-central1",
        batch_id=f"bronze-silver-vacinacao-covid19-{now.strftime("%Y%m%d%H%M%S")}",
    )
    
    silver_gold_dm_campanhas = DataprocCreateBatchOperator(
        task_id=f"create_job_silver_gold_dm_campanhas",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://pgii-dataproc/gold/silver_gold_dm_campanhas.py",
                "args": ["--year", str(ingestion_date.year), "--month", str(ingestion_date.month), "--day", str(ingestion_date.day)]
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
        batch_id=f"silver-gold-dm-campanhas-{now.strftime("%Y%m%d%H%M%S")}",
    )
    
    silver_gold_dm_estabelecimentos = DataprocCreateBatchOperator(
        task_id=f"create_job_silver_gold_dm_estabelecimentos",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://pgii-dataproc/gold/silver_gold_dm_estabelecimentos.py",
                "args": ["--year", str(ingestion_date.year), "--month", str(ingestion_date.month), "--day", str(ingestion_date.day)]
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
        batch_id=f"silver-gold-dm-estabelecimentos-{now.strftime("%Y%m%d%H%M%S")}",
    )
    
    silver_gold_dm_vacinas = DataprocCreateBatchOperator(
        task_id=f"create_job_silver_gold_dm_vacinas",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://pgii-dataproc/gold/silver_gold_dm_vacinas.py",
                "args": ["--year", str(ingestion_date.year), "--month", str(ingestion_date.month), "--day", str(ingestion_date.day)]
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
        batch_id=f"silver-gold-dm-vacinas-{now.strftime("%Y%m%d%H%M%S")}",
    )
    
    silver_gold_dm_municipios = DataprocCreateBatchOperator(
        task_id=f"create_job_silver_gold_dm_municipios",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://pgii-dataproc/gold/silver_gold_dm_municipios.py",
                "args": ["--year", str(ingestion_date.year), "--month", str(ingestion_date.month), "--day", str(ingestion_date.day)]
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
        batch_id=f"silver-gold-dm-municipios-{now.strftime("%Y%m%d%H%M%S")}",
    )

    silver_gold_dm_pacientes = DataprocCreateBatchOperator(
        task_id=f"create_job_silver_gold_dm_pacientes",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://pgii-dataproc/gold/silver_gold_dm_pacientes.py",
                "args": ["--year", str(ingestion_date.year), "--month", str(ingestion_date.month), "--day", str(ingestion_date.day)]
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
        batch_id=f"silver-gold-dm-pacientes-{now.strftime("%Y%m%d%H%M%S")}",
    )

    silver_gold_ft_vacinacao = DataprocCreateBatchOperator(
        task_id=f"create_job_silver_gold_ft_vacinacao",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://pgii-dataproc/gold/silver_gold_ft_vacinacao.py",
                "args": ["--year", str(ingestion_date.year), "--month", str(ingestion_date.month), "--day", str(ingestion_date.day)]
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
        batch_id=f"silver-gold-ft-vacinacao-{now.strftime("%Y%m%d%H%M%S")}",
    )
   
    ingestion_task >> bronze_silver_task >>  \
    silver_gold_dm_campanhas >> silver_gold_dm_estabelecimentos >> \
    silver_gold_dm_vacinas >> silver_gold_dm_municipios >> \
    silver_gold_dm_pacientes >> silver_gold_ft_vacinacao
    
    