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

# %%
tablename = 'dm_estabelecimentos'
data_path = Path(os.environ["DATA_PATH"])

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
    .load("/".join([os.environ["DATA_PATH"], 'trusted/vacinacao_covid19/csv']))
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
df_estabelecimentos.write.mode('overwrite').format('parquet').save(f'./data/refined/{tablename}')

# %%
df_estabelecimentos.write.format('jdbc').options(
    url=f'jdbc:postgresql://{os.environ["POSTGRES_ADDRESS"]}/{os.environ["POSTGRES_DB"]}',
    dbtable=tablename,
    driver='org.postgresql.Driver',
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"]
).mode('overwrite').save()