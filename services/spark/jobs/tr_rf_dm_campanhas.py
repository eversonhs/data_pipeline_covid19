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
from pyspark.sql.functions import col, lit, xxhash64
from pathlib import Path
import os

# %% [markdown]
# ### 1.2. Configuração do contexto Spark

# %%
spark = SparkSession.Builder() \
    .master(os.environ["SPARK_MASTER_URI"]) \
    .appName("covid_19_vacination_dm_campanhas") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()

# %%
data_path = Path(os.environ["DATA_PATH"])

# %% [markdown]
# ## 2.0. Leitura dos dados

# %%
df_campanhas = (
    spark
    .read
    .format('parquet')
    .load("/".join([os.environ["DATA_PATH"], 'trusted/vacinacao_covid19']))
    .select(
        col('vacina_grupoAtendimento_codigo').alias('CD_GRUPO_ATENDIMENTO'),
        col('vacina_grupoAtendimento_nome').alias('NM_GRUPO_ATENDIMENTO'),
        col('vacina_categoria_codigo').alias('CD_CATEGORIA_GRUPO_ATENDIMENTO'),
        col('vacina_categoria_nome').alias('NM_CATEGORIA_GRUPO_ATENDIMENTO'),
        col('vacina_descricao_dose').alias('DSC_DOSE')
    )
    .distinct()
)
# %%
df_campanhas = (
    df_campanhas
    .withColumn('SK_DM_CAMPANHAS', xxhash64('CD_GRUPO_ATENDIMENTO', 'CD_CATEGORIA_GRUPO_ATENDIMENTO', 'DSC_DOSE'))
)

# %%
df_campanhas.write.mode('overwrite').format('parquet').save('./data/refined/dm_campanhas')

# %%
df_campanhas.write.format('jdbc').options(
    url=f'jdbc:mysql://{os.environ["MYSQL_ADDRESS"]}/{os.environ["MYSQL_DATABASE"]}',
    dbtable='dm_campanhas',
    driver='com.mysql.cj.jdbc.Driver',
    user=os.environ["MYSQL_USER"],
    password=os.environ["MYSQL_PASSWORD"]
).mode('overwrite').save()