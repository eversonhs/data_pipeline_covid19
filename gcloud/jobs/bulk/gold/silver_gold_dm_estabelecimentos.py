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
temp_bucket = "gs://pgii-dataproc-temp"
input_directory = f"gs://pgii-silver/vacinacao_covid19/{{{exec_args.year}}}/{{{exec_args.month}}}/{{{exec_args.day}}}/*"
tablename = 'dm_estabelecimentos'
output_directory = f"gs://pgii-gold/{tablename}"

spark = SparkSession.builder \
    .appName(f"covid_19_vacination_{tablename}") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', temp_bucket)

df_estabelecimentos = (
    spark
    .read
    .format('parquet')
    .load(input_directory)
    .select(
        col('estabelecimento_valor').alias('CD_ESTABELECIMENTO'),
        col('estabelecimento_razaoSocial').alias('DSC_RAZAO_SOCIAL'),
        col('estalecimento_noFantasia').alias('NM_FANTASIA'),
    )
    .dropDuplicates(subset=['CD_ESTABELECIMENTO'])
)

df_estabelecimentos = (
    df_estabelecimentos
    .withColumn('SK_DM_ESTABELECIMENTOS', xxhash64('CD_ESTABELECIMENTO'))
)

# %%
# Merge operation
if DeltaTable.isDeltaTable(spark, output_directory):
    df_estabelecimentos_old = DeltaTable.forPath(spark, output_directory)
    (
        df_estabelecimentos_old
        .alias("target")
        .merge(df_estabelecimentos.alias("source"), "target.SK_DM_ESTABELECIMENTOS=source.SK_DM_ESTABELECIMENTOS")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    df_estabelecimentos.write.mode('overwrite').format('delta').save(output_directory)
    
# %%
dm_estabelecimentos = spark.read.format("delta").load(output_directory)
(
    dm_estabelecimentos
    .write
    .format('bigquery')
    .option('table', f'ds_pgii.{tablename}')
    .mode('overwrite')
    .save()
)