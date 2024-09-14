# %% [markdown]
# # Tabela dm_vacinas de dados de vacinação contra COVID-19

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

# %%
tablename = 'dm_vacinas'
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
df_vacina = (
    spark
    .read
    .format('parquet')
    .load(input_directory)
    .select(
        col('vacina_codigo').alias('CD_VACINA'),
        col('vacina_lote').alias('DSC_LOTE'),
        col('vacina_fabricante_nome').alias('NM_FABRICANTE'),
        col('vacina_nome').alias('NM_VACINA')
    )
    .dropDuplicates(subset=['CD_VACINA', 'DSC_LOTE'])
)
# %%
df_vacina = (
    df_vacina
    .withColumn('SK_DM_VACINAS', xxhash64('CD_VACINA', 'DSC_LOTE'))
)

# %%
df_vacina.write.mode('overwrite').format('parquet').save(output_directory)

# %%
(
    df_vacina
    .write
    .format('bigquery')
    .option('table', f'ds_pgii.{tablename}')
    .mode('overwrite')
    .save()
)