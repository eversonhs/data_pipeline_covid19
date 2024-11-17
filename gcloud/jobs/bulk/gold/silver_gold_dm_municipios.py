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
from pyspark.sql.functions import col, xxhash64, lit
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
tablename = 'dm_municipios'
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
df_vacinacao = (
    spark
    .read
    .format('parquet')
    .load(input_directory)
)

# %%
df_enderecos_estabelecimentos = (
    df_vacinacao
    .select(
        col('estabelecimento_municipio_codigo').alias('CD_MUNICIPIO_IBGE'),
        lit(10).alias('CD_PAIS'),
        col('estabelecimento_municipio_nome').alias('NM_MUNICIPIO'),
        lit('BRASIL').alias('NM_PAIS'),
        col('estabelecimento_uf').alias('SG_UF')
    )
    .dropDuplicates(subset=['CD_MUNICIPIO_IBGE'])
)

# %%
df_enderecos_pacientes = (
    df_vacinacao
    .select(
        col('paciente_endereco_coIbgeMunicipio').alias('CD_MUNICIPIO_IBGE'),
        col('paciente_endereco_coPais').alias('CD_PAIS'),
        col('paciente_endereco_nmMunicipio').alias('NM_MUNICIPIO'),
        col('paciente_endereco_nmPais').alias('NM_PAIS'),
        col('paciente_endereco_uf').alias('SG_UF')
    )
    .dropDuplicates(subset=['CD_MUNICIPIO_IBGE', 'CD_PAIS'])
)
# %%
df_municipios = (
    df_enderecos_estabelecimentos
    .unionByName(df_enderecos_pacientes)
    .dropDuplicates(subset=['CD_MUNICIPIO_IBGE', 'CD_PAIS'])
    .dropna(subset=['CD_PAIS', 'NM_MUNICIPIO'], how='all')
)

# %%
df_municipios = (
    df_municipios
    .withColumn('SK_DM_MUNICIPIOS', xxhash64('CD_MUNICIPIO_IBGE', 'CD_PAIS'))
)

# %%
# Merge operation
if DeltaTable.isDeltaTable(spark, output_directory):
    df_municipios_old = DeltaTable.forPath(spark, output_directory)
    (
        df_municipios_old
        .alias("target")
        .merge(df_municipios.alias("source"), "target.SK_DM_MUNICIPIOS=source.SK_DM_MUNICIPIOS")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    df_municipios.write.mode('overwrite').format('delta').save(output_directory)
    
# %%
dm_municipios = spark.read.format("delta").load(output_directory)

# %%
(
    dm_municipios
    .write
    .format('bigquery')
    .option('table', f'ds_pgii.{tablename}')
    .mode('overwrite')
    .save()
)