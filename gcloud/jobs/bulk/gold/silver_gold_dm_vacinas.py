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
from delta.tables import DeltaTable
import argparse

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--year',
        help="Year of execution",
        default='*'
    )
    parser.add_argument(
        '--month',
        help="month of execution",
        default='*'
    )
    parser.add_argument(
        '--day',
        help="day of execution",
        default='*'
    )

    known_args = parser.parse_args()
    return known_args

# %% [markdown]
# ### 1.2. Configuração do contexto Spark

# %%
exec_args = parse_args()
# %%
tablename = 'dm_vacinas'
temp_bucket = "gs://pgii-dataproc-temp"
input_directory = f"gs://pgii-silver/vacinacao_covid19/{{{exec_args.year}}}/{{{exec_args.month}}}/{{{exec_args.day}}}/*"
output_directory = f"gs://pgii-gold/{tablename}"

# %% [markdown]
# ### 1.2. Configuração do contexto Spark

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
df_vacina = (
    spark
    .read
    .format('parquet')
    .load(input_directory)
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
# Merge operation
if DeltaTable.isDeltaTable(spark, output_directory):
    df_vacina_old = DeltaTable.forPath(spark, output_directory)
    (
        df_vacina_old
        .alias("target")
        .merge(df_vacina.alias("source"), "target.SK_DM_VACINAS=source.SK_DM_VACINAS")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    df_vacina.write.mode('overwrite').format('delta').save(output_directory)
    
# %%
dm_vacinas = spark.read.format("delta").load(output_directory)

# %%
(
    dm_vacinas
    .write
    .format('bigquery')
    .option('table', f'ds_pgii.{tablename}')
    .mode('overwrite')
    .save()
)