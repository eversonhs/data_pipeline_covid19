from pyspark.sql import SparkSession
from pyspark.sql.functions import col, xxhash64


temp_bucket = "gs://pgii-dataproc-temp/"
input_directory = "gs://pgii-trusted/vacinacao_covid19/csv/*"
tablename = 'dm_estabelecimentos'
output_directory = f"gs://pgii-refined/{tablename}"

spark = SparkSession.builder \
    .appName(f"covid_19_vacination_{tablename}") \
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

df_estabelecimentos.write.mode('overwrite').format('parquet').save(output_directory)

(
    df_estabelecimentos
    .write
    .format('bigquery')
    .option('table', f'ds_pgii.{tablename}')
    .mode('overwrite')
    .save()
)