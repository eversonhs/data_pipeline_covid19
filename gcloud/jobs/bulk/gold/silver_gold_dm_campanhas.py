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
from delta.tables import DeltaTable
import argparse

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--date',
        help="date of execution",
        default='*'
    )
    known_args = parser.parse_args()
    return known_args

# %% [markdown]
# ### 1.2. Configuração do contexto Spark

# %%
exec_args = parse_args()
temp_bucket = "gs://pgii-dataproc-temp"
exec_year, exec_month, exec_day = tuple(exec_args.date.split("-")) if exec_args.date != "*" else ("*", "*", "*")
input_directory = f"gs://pgii-silver/vacinacao_covid19/{{{exec_year}}}/{{{exec_month}}}/{{{exec_day}}}/*"
tablename = 'dm_campanhas'
output_directory = f"gs://pgii-gold/{tablename}"

# %%
spark = SparkSession.builder \
    .appName(f"covid_19_vacination_{tablename}") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
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
# Merge operation
if DeltaTable.isDeltaTable(spark, output_directory):
    df_campanhas_old = DeltaTable.forPath(spark, output_directory)
    (
        df_campanhas_old
        .alias("target")
        .merge(df_campanhas.alias("source"), "target.SK_DM_CAMPANHAS=source.SK_DM_CAMPANHAS")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    df_campanhas.write.mode('overwrite').format('delta').save(output_directory)
    

# %%
dm_campanhas = spark.read.format("delta").load(output_directory)
(
    dm_campanhas
    .write
    .format('bigquery')
    .option('table', f'ds_pgii.{tablename}')
    .mode('overwrite')
    .save()
)