# %% [markdown]
# # Tabela dm_pacientes de dados de vacinação contra COVID-19

# %% [markdown]
# ## 1.0. Configurações

# %% [markdown]
# ### 1.1. Importação das dependências necessárias

# %% [markdown]
# - Pyspark 3.4.1

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, xxhash64
from pathlib import Path
import os
import argparse
from datetime import datetime

# %%
# Execution Date
parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str, required=True)
known_args, args_cli = parser.parse_known_args()
ingest_date = datetime.strptime(known_args.date, "%Y-%m-%d")

# %%
input_directory = f'{os.environ["DATA_PATH"]}/trusted/vacinacao_covid19/{ingest_date.year:4d}/{ingest_date.month:02d}/{ingest_date.day:02d}'
tablename = 'dm_pacientes'

# %% [markdown]
# ### 1.2. Configuração do contexto Spark

# %%
spark = SparkSession.Builder() \
    .master(os.environ["SPARK_MASTER_URI"]) \
    .appName(f"covid_19_vacination_{tablename}") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()

# %% [markdown]
# ## 2.0. Leitura dos dados

# %%
df_paciente = (
    spark
    .read
    .format('parquet')
    .load(input_directory)
    .select(
        col('paciente_id').alias('CD_PACIENTE'),
        col('paciente_dataNascimento').alias('DT_NASCIMENTO'),
        col('paciente_enumSexoBiologico').alias('SG_SEXO_BIOLOGICO'),
        col('paciente_racaCor_codigo').alias('CD_RACA_COR'),
        col('paciente_racaCor_valor').alias('DSC_RACA_CORD'),
        col('paciente_nacionalidade_enumNacionalidade').alias('CD_NACIONALIDADE')
    )
    .dropDuplicates(subset=['CD_PACIENTE', 'CD_RACA_COR'])
)
# %%
df_paciente = (
    df_paciente
    .withColumn('SK_DM_PACIENTES', xxhash64('CD_PACIENTE', 'CD_RACA_COR'))
)

# %%
output_folder = f'{os.environ["DATA_PATH"]}/refined/{tablename}'
# Merge operation
df_paciente_old = spark.read.format('parquet').load(output_folder)
df_paciente_diff = df_paciente_old.join(df_paciente, on='SK_DM_PACIENTES', how='anti_left')
df_paciente = df_paciente.unionByName(df_paciente_diff)
df_paciente.write.mode('overwrite').format('parquet').save(output_folder)
# %%
df_paciente.write.format('jdbc').options(
    url=f'jdbc:postgresql://{os.environ["POSTGRES_ADDRESS"]}/{os.environ["POSTGRES_DB"]}',
    dbtable=tablename,
    driver='org.postgresql.Driver',
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"]
).mode('overwrite').save()