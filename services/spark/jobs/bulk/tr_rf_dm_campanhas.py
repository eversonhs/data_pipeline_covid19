# %% [markdown]
# # Tabela dm_campanhas de dados de vacinação contra COVID-19

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

# %% [markdown]
# ### 1.2. Configuração do contexto Spark

# %%
data_path = Path(os.environ["DATA_PATH"])
tablename = 'dm_campanhas'

# %%
spark = SparkSession.Builder() \
    .master(os.environ["SPARK_MASTER_URI"]) \
    .appName(f"covid_19_vacination_{tablename}") \
    .config("spark.driver.maxResultSize", "8g") \
    .getOrCreate()

# %% [markdown]
# ## 2.0. Leitura dos dados

# %%
df_campanhas = (
    spark
    .read
    .format('parquet')
    .load("/".join([os.environ["DATA_PATH"], 'trusted/vacinacao_covid19/csv']))
    .select(
        col('vacina_grupoAtendimento_codigo').alias('CD_GRUPO_ATENDIMENTO'),
        col('vacina_grupoAtendimento_nome').alias('NM_GRUPO_ATENDIMENTO'),
        col('vacina_categoria_codigo').alias('CD_CATEGORIA_GRUPO_ATENDIMENTO'),
        col('vacina_categoria_nome').alias('NM_CATEGORIA_GRUPO_ATENDIMENTO'),
        col('vacina_descricao_dose').alias('DSC_DOSE')
    )
    .distinct()
    .dropna(subset=['NM_GRUPO_ATENDIMENTO'])
    .dropDuplicates(['CD_GRUPO_ATENDIMENTO', 'CD_CATEGORIA_GRUPO_ATENDIMENTO', 'DSC_DOSE'])
)
# %%
df_campanhas = (
    df_campanhas
    .withColumn('SK_DM_CAMPANHAS', xxhash64('CD_GRUPO_ATENDIMENTO', 'CD_CATEGORIA_GRUPO_ATENDIMENTO', 'DSC_DOSE'))
)

# %%
df_campanhas.write.mode('overwrite').format('parquet').save(f'./data/refined/{tablename}')

# %%
df_campanhas.write.format('jdbc').options(
    url=f'jdbc:postgresql://{os.environ["POSTGRES_ADDRESS"]}/{os.environ["POSTGRES_DB"]}',
    dbtable=tablename,
    driver='org.postgresql.Driver',
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"]
).mode('overwrite').save()