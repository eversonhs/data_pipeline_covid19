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
from pyspark.sql.functions import col, xxhash64, row_number
from delta.tables import DeltaTable
from pyspark.sql.window import Window
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
tablename = 'dm_pacientes'
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
        col('paciente_nacionalidade_enumNacionalidade').alias('CD_NACIONALIDADE'),
        col('vacina_dataAplicacao').alias("DT_VACINACAO")
    )
)

# %%
# Mantém apenas os registros mais recentes de cada paciente
window_spec = Window.partitionBy("CD_PACIENTE").orderBy(col("DT_VACINACAO").desc())

df_paciente = (
    df_paciente
    .select(
        '*',
        (row_number().over(window_spec)).alias('rank')
    )
    .filter(col('rank') == 1)
    .drop('rank')
    .drop('DT_VACINACAO')
)

# %%
df_paciente = (
    df_paciente
    .withColumn('SK_DM_PACIENTES', xxhash64('CD_PACIENTE'))
)

# %%
# Merge operation
if DeltaTable.isDeltaTable(spark, output_directory):
    df_paciente_old = DeltaTable.forPath(spark, output_directory)
    (
        df_paciente_old
        .alias("target")
        .merge(df_paciente.alias("source"), "target.SK_DM_PACIENTES=source.SK_DM_PACIENTES")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    df_paciente.write.mode('overwrite').format('delta').save(output_directory)
    
# %%
dm_pacientes = spark.read.format("delta").load(output_directory)

# %%
(
    dm_pacientes
    .write
    .format('bigquery')
    .option('table', f'ds_pgii.{tablename}')
    .mode('overwrite')
    .save()
)