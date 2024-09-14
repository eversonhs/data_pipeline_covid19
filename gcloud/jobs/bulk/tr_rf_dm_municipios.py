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
import os

# %%
tablename = 'dm_municipios'
temp_bucket = "gs://pgii-dataproc-temp"
input_directory = "gs://pgii-trusted/vacinacao_covid19/csv/*"
output_directory = f"gs://pgii-refined/{tablename}"

# %% [markdown]
# ### 1.2. Configuração do contexto Spark

# %%
spark = SparkSession.builder \
    .appName(f"covid_19_vacination_{tablename}") \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', temp_bucket)



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

# %%
dm_municipios.write.mode('overwrite').format('parquet').save(output_directory)

# %%
(
    dm_municipios
    .write
    .format('bigquery')
    .option('table', f'ds_pgii.{tablename}')
    .mode('overwrite')
    .save()
)