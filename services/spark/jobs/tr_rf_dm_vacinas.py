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
    .appName("covid_19_vacination_data_cleaning") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()

# %%
data_path = Path(os.environ["DATA_PATH"])

# %% [markdown]
# ## 2.0. Leitura dos dados

# %%
df_vacina = (
    spark
    .read
    .format('parquet')
    .load("/".join([os.environ["DATA_PATH"], 'trusted/vacinacao_covid19']))
    .select(
        col('vacina_codigo').alias('CD_VACINA'),
        col('vacina_lote').alias('DSC_LOTE'),
        col('vacina_fabricante_nome').alias('NM_FABRICANTE'),
        col('vacina_nome').alias('NM_VACINA')
    )
    .distinct()
)
# %%
df_vacina = (
    df_vacina
    .withColumn('SK_DM_VACINA', xxhash64('CD_VACINA', 'DSC_LOTE'))
)

# %%
df_vacina.write.format('parquet').save('./data/refined/dm_vacina')

# %%
df_vacina.write.format('jdbc').options(
    url=f'jdbc:mysql://{os.environ["MYSQL_ADDRESS"]}/{os.environ["MYSQL_DATABASE"]}',
    dbtable='dm_vacinas',
    driver='com.mysql.cj.jdbc.Driver',
    user=os.environ["MYSQL_USER"],
    password=os.environ["MYSQL_PASSWORD"]
).mode('overwrite').save()