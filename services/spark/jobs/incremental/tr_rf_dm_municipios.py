# %% [markdown]
# # Tabela dm_municipios de dados de vacinação contra COVID-19

# %% [markdown]
# ## 1.0. Configurações

# %% [markdown]
# ### 1.1. Importação das dependências necessárias

# %% [markdown]
# - Pyspark 3.4.1

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, xxhash64, lit
from pathlib import Path
from datetime import datetime
import argparse
import os

# %%
# Execution Date
parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str, required=True)
known_args, args_cli = parser.parse_known_args()
ingest_date = datetime.strptime(known_args.date, "%Y-%m-%d")

# %%
tablename = 'dm_municipios'
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
df_vacinacao = (
    spark
    .read
    .format('parquet')
    .load(input_directory)
)

# %%
df_enderecos_estabelecimentos = (
    df_vacinacao
    .select(
        col('estabelecimento_municipio_codigo').alias('CD_MUNICIPIO_IBGE'),
        lit(10).alias('CD_PAIS'),
        col('estabelecimento_municipio_nome').alias('NM_MUNICIPIO'),
        lit('BRASIL').alias('NM_PAIS'),
        col('estabelecimento_uf').alias('SG_UF')
    )
    .dropDuplicates(subset=['CD_MUNICIPIO_IBGE'])
)

# %%
df_enderecos_pacientes = (
    df_vacinacao
    .select(
        col('paciente_endereco_coIbgeMunicipio').alias('CD_MUNICIPIO_IBGE'),
        col('paciente_endereco_coPais').alias('CD_PAIS'),
        col('paciente_endereco_nmMunicipio').alias('NM_MUNICIPIO'),
        col('paciente_endereco_nmPais').alias('NM_PAIS'),
        col('paciente_endereco_uf').alias('SG_UF')
    )
    .dropDuplicates(subset=['CD_MUNICIPIO_IBGE', 'CD_PAIS'])
)
# %%
dm_municipios = (
    df_enderecos_estabelecimentos
    .unionByName(df_enderecos_pacientes)
    .dropDuplicates(subset=['CD_MUNICIPIO_IBGE', 'CD_PAIS'])
    .dropna(subset=['CD_PAIS', 'NM_MUNICIPIO'], how='all')
)

# %%
dm_municipios = (
    dm_municipios
    .withColumn('SK_DM_MUNICIPIOS', xxhash64('CD_MUNICIPIO_IBGE', 'CD_PAIS'))
)

output_folder = f'{os.environ["DATA_PATH"]}/refined/{tablename}'

# %%
# Merge operation
dm_municipios_old = spark.read.format('parquet').load(output_folder)
dm_municipios_diff = dm_municipios_old.join(dm_municipios, on='SK_DM_MUNICIPIOS', how='anti_left')
dm_municipios = dm_municipios.unionByName(dm_municipios_diff)
dm_municipios.write.mode('overwrite').format('parquet').save(output_folder)

# %%
dm_municipios.write.format('jdbc').options(
    url=f'jdbc:postgresql://{os.environ["POSTGRES_ADDRESS"]}/{os.environ["POSTGRES_DB"]}',
    dbtable=tablename,
    driver='org.postgresql.Driver',
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"]
).mode('overwrite').save()