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

# %% [markdown]
# ### 1.2. Configuração do contexto Spark

# %%
temp_bucket = "gs://pgii-dataproc-temp"
input_directory = "gs://pgii-silver/vacinacao_covid19/csv/*"
tablename = 'dm_campanhas'
output_directory = f"gs://pgii-gold/{tablename}"

# %%
spark = SparkSession.builder \
    .appName(f"covid_19_vacination_{tablename}") \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', temp_bucket)

# %% [markdown]
# ## 2.0. Leitura dos dados

# %%
df_campanhas = (
    spark
    .read
    .format('parquet')
    .load(input_directory)
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
df_campanhas.write.mode('overwrite').format('parquet').save(output_directory)

# %%
(
    df_campanhas
    .write
    .format('bigquery')
    .option('table', f'ds_pgii.{tablename}')
    .mode('overwrite')
    .save()
)