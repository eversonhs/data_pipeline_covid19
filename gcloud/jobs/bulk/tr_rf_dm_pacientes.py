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

# %%
temp_bucket = "gs://pgii-dataproc-temp"
input_directory = "gs://pgii-trusted/vacinacao_covid19/csv/*"
tablename = 'dm_pacientes'
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
df_paciente = (
    spark
    .read
    .format('parquet')
    .load(input_directory)
    .select(
        col('paciente_id').alias('CD_PACIENTE'),
        col('paciente_dataNascimento').alias('DT_NASCIMENTO'),
        col('paciente_enumSexoBiologico').alias('SG_SEXO_BIOLOGICO'),
        col('paciente_racaCor_codigo').alias('CD_RACA_COR'),
        col('paciente_racaCor_valor').alias('DSC_RACA_CORD'),
        col('paciente_nacionalidade_enumNacionalidade').alias('CD_NACIONALIDADE')
    )
    .dropDuplicates(subset=['CD_PACIENTE', 'CD_RACA_COR'])
)
# %%
df_paciente = (
    df_paciente
    .withColumn('SK_DM_PACIENTES', xxhash64('CD_PACIENTE', 'CD_RACA_COR'))
)

# %%
df_paciente.write.mode('overwrite').format('parquet').save(output_directory)

# %%
(
    df_paciente
    .write
    .format('bigquery')
    .option('table', f'ds_pgii.{tablename}')
    .mode('overwrite')
    .save()
)