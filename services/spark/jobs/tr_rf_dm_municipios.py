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
tablename = 'dm_municipios'
data_path = Path(os.environ["DATA_PATH"])

# %% [markdown]
# ## 2.0. Leitura dos dados

# %%
df_enderecos_pacientes = (
    spark
    .read
    .format('parquet')
    .load("/".join([os.environ["DATA_PATH"], 'trusted/vacinacao_covid19']))
    .select(
        col('paciente_endereco_coIbgeMunicipio').alias('CD_MUNICIPIO_IBGE'),
        col('paciente_endereco_coPais').alias('CD_PAIS'),
        col('paciente_endereco_nmMunicipio').alias('NM_MUNICIPIO'),
        col('paciente_endereco_nmPais').alias('NM_PAIS'),
        col('paciente_endereco_uf').alias('SG_UF'),
        col('paciente_endereco_cep').alias('CD_CEP'),
    )
    .distinct()
)

# %%
df_enderecos_pacientes = (
    df_enderecos_pacientes
    .withColumn('SK_DM_MUNICIPIOS', xxhash64('CD_MUNICIPIO_IBGE', 'CD_CEP'))
)

# %%
df_enderecos_pacientes.write.mode('overwrite').format('parquet').save(f'./data/refined/{tablename}')

# %%
df_enderecos_pacientes.write.format('jdbc').options(
    url=f'jdbc:mysql://{os.environ["MYSQL_ADDRESS"]}/{os.environ["MYSQL_DATABASE"]}',
    dbtable=tablename,
    driver='com.mysql.cj.jdbc.Driver',
    user=os.environ["MYSQL_USER"],
    password=os.environ["MYSQL_PASSWORD"]
).mode('overwrite').save()