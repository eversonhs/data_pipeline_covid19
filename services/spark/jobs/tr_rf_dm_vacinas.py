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
data_path = Path(os.environ["DATA_PATH"])
tablename = 'dm_vacinas'

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
df_vacina = (
    spark
    .read
    .format('parquet')
    .load("/".join([os.environ["DATA_PATH"], 'trusted/vacinacao_covid19/csv']))
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
df_vacina.write.mode('overwrite').format('parquet').save(f'./data/refined/{tablename}')

# %%
df_vacina.write.format('jdbc').options(
    url=f'jdbc:postgresql://{os.environ["POSTGRES_ADDRESS"]}/{os.environ["POSTGRES_DB"]}',
    dbtable=tablename,
    driver='org.postgresql.Driver',
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"]
).mode('overwrite').save()