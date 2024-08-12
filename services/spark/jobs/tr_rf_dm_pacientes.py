# %% [markdown]
# # Tabela dm_pacientes de dados de vacinação contra COVID-19

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
tablename = 'dm_pacientes'

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
df_paciente = (
    spark
    .read
    .format('parquet')
    .load("/".join([os.environ["DATA_PATH"], 'trusted/vacinacao_covid19']))
    .select(
        col('paciente_id').alias('CD_PACIENTE'),
        col('paciente_idade').alias('VL_IDADE'),
        col('paciente_dataNascimento').alias('DT_NASCIMENTO'),
        col('paciente_enumSexoBiologico').alias('SG_SEXO_BIOLOGICO'),
        col('paciente_racaCor_codigo').alias('CD_RACA_COR'),
        col('paciente_racaCor_valor').alias('DSC_RACA_CORD'),
        col('paciente_nacionalidade_enumNacionalidade').alias('CD_NACIONALIDADE')
    )
    .distinct()
)
# %%
df_paciente = (
    df_paciente
    .withColumn('SK_DM_PACIENTE', xxhash64('CD_PACIENTE'))
)

# %%
df_paciente.write.mode('overwrite').format('parquet').save(f'./data/refined/{tablename}')

# %%
df_paciente.write.format('jdbc').options(
    url=f'jdbc:mysql://{os.environ["MYSQL_ADDRESS"]}/{os.environ["MYSQL_DATABASE"]}',
    dbtable=tablename,
    driver='com.mysql.cj.jdbc.Driver',
    user=os.environ["MYSQL_USER"],
    password=os.environ["MYSQL_PASSWORD"]
).mode('overwrite').save()