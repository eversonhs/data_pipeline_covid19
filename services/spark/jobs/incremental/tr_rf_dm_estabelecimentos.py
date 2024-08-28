# %% [markdown]
# # Tabela dm_estabelecimentos de dados de vacinação contra COVID-19

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
tablename = 'dm_estabelecimentos'
input_directory = f'{os.environ["DATA_PATH"]}/trusted/vacinacao_covid19/{ingest_date.year:4d}/{ingest_date.month:02d}/{ingest_date.day:02d}'

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
df_estabelecimentos = (
    spark
    .read
    .format('parquet')
    .load(input_directory)
    .select(
        col('estabelecimento_valor').alias('CD_ESTABELECIMENTO'),
        col('estabelecimento_razaoSocial').alias('DSC_RAZAO_SOCIAL'),
        col('estalecimento_noFantasia').alias('NM_FANTASIA'),
    )
    .dropDuplicates(subset=['CD_ESTABELECIMENTO'])
)

# %%
df_estabelecimentos = (
    df_estabelecimentos
    .withColumn('SK_DM_ESTABELECIMENTOS', xxhash64('CD_ESTABELECIMENTO'))
)

# %%
output_folder = f'{os.environ["DATA_PATH"]}/refined/{tablename}'

# %%
# Merge operation
df_estabelecimentos_old = spark.read.format('parquet').load(output_folder)
df_estabelecimentos_diff = df_estabelecimentos_old.join(df_estabelecimentos, on='SK_DM_ESTABELECIMENTOS', how='anti_left')
df_estabelecimentos = df_estabelecimentos.unionByName(df_estabelecimentos_diff)
df_estabelecimentos.write.mode('overwrite').format('parquet').save(output_folder)

# %%
df_estabelecimentos.write.format('jdbc').options(
    url=f'jdbc:postgresql://{os.environ["POSTGRES_ADDRESS"]}/{os.environ["POSTGRES_DB"]}',
    dbtable=tablename,
    driver='org.postgresql.Driver',
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"]
).mode('overwrite').save()