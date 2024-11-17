# %% [markdown]
# # Tabela ft_vacinacao de dados de vacinação contra COVID-19

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

# %%
exec_args = parse_args()
tablename = 'ft_vacinacao'
temp_bucket = "gs://pgii-dataproc-temp"
output_directory = f"gs://pgii-gold/{tablename}"
input_directory = f"gs://pgii-silver/vacinacao_covid19/{{{exec_args.year}}}/{{{exec_args.month}}}/{{{exec_args.day}}}/*"
input_directory_1 = "gs://pgii-gold/dm_pacientes"
input_directory_2 = "gs://pgii-gold/dm_vacinas"
input_directory_3 = "gs://pgii-gold/dm_campanhas"
input_directory_4 = "gs://pgii-gold/dm_estabelecimentos"
input_directory_5 = "gs://pgii-gold/dm_municipios"

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
    .withColumnRenamed('paciente_id', 'CD_PACIENTE')
    .withColumnRenamed('vacina_grupoAtendimento_codigo', 'CD_GRUPO_ATENDIMENTO')
    .withColumnRenamed('vacina_categoria_codigo', 'CD_CATEGORIA_GRUPO_ATENDIMENTO')
    .withColumnRenamed('vacina_descricao_dose', 'DSC_DOSE')
    .withColumnRenamed('estabelecimento_valor', 'CD_ESTABELECIMENTO')
    .withColumnRenamed('paciente_endereco_coPais', 'CD_PAIS_PACIENTE')
    .withColumnRenamed('paciente_endereco_coIbgeMunicipio', 'CD_MUNICIPIO_IBGE_PACIENTE')
    .withColumn('CD_PAIS_ESTABELECIMENTO', lit(10))
    .withColumnRenamed('estabelecimento_municipio_codigo', 'CD_MUNICIPIO_IBGE_ESTABELECIMENTO')
    .withColumnRenamed('vacina_lote', 'DSC_LOTE')
    .withColumnRenamed('vacina_codigo', 'CD_VACINA')
)
# %%
dm_pacientes = (
    spark
    .read
    .format('delta')
    .load(input_directory_1)
    .select(
        'CD_PACIENTE',
        'SK_DM_PACIENTES'
    )
)
# %%
dm_vacinas = (
    spark
    .read
    .format('delta')
    .load(input_directory_2)
    .select(
        'CD_VACINA',
        'DSC_LOTE',
        'SK_DM_VACINAS'
    )
)
# %%
dm_campanhas = (
    spark
    .read
    .format('delta')
    .load(input_directory_3)
    .select(
        'CD_GRUPO_ATENDIMENTO',
        'CD_CATEGORIA_GRUPO_ATENDIMENTO',
        'DSC_DOSE',
        'SK_DM_CAMPANHAS'
    )
)
# %%
dm_estabelecimentos = (
    spark
    .read
    .format('delta')
    .load(input_directory_4)
    .select(
        'CD_ESTABELECIMENTO',
        'SK_DM_ESTABELECIMENTOS'
    )
)
# %%
dm_municipios = (
    spark
    .read
    .format('delta')
    .load(input_directory_5)
    .select(
        'CD_MUNICIPIO_IBGE',
        'CD_PAIS',
        'SK_DM_MUNICIPIOS'
    )
)
# %%
df_vacinacao = (
    df_vacinacao
    .join(dm_pacientes, on='CD_PACIENTE', how='left')
    .join(dm_vacinas, on=['CD_VACINA', 'DSC_LOTE'], how='left')
    .join(dm_campanhas, on=['CD_GRUPO_ATENDIMENTO', 'CD_CATEGORIA_GRUPO_ATENDIMENTO', 'DSC_DOSE'], how='left')
    .join(dm_estabelecimentos, on=['CD_ESTABELECIMENTO'], how='left')
    .join(
        (
            dm_municipios
            .withColumnRenamed('SK_DM_MUNICIPIOS', 'SK_DM_MUNICIPIOS_PACIENTE')
            .withColumnRenamed('CD_MUNICIPIO_IBGE', 'CD_MUNICIPIO_IBGE_PACIENTE')
            .withColumnRenamed('CD_PAIS', 'CD_PAIS_PACIENTE')
        ),
        on=['CD_MUNICIPIO_IBGE_PACIENTE', 'CD_PAIS_PACIENTE'],
        how='left'
    )
    .join(
        (
            dm_municipios
            .withColumnRenamed('SK_DM_MUNICIPIOS', 'SK_DM_MUNICIPIOS_ESTABELECIMENTO')
            .withColumnRenamed('CD_MUNICIPIO_IBGE', 'CD_MUNICIPIO_IBGE_ESTABELECIMENTO')
            .withColumnRenamed('CD_PAIS', 'CD_PAIS_ESTABELECIMENTO')
        ),
        on=['CD_MUNICIPIO_IBGE_ESTABELECIMENTO', 'CD_PAIS_ESTABELECIMENTO'],
        how='left'
    )
    .select(
        'SK_DM_PACIENTES',
        'SK_DM_VACINAS',
        'SK_DM_CAMPANHAS',
        'SK_DM_ESTABELECIMENTOS',
        'SK_DM_MUNICIPIOS_PACIENTE',
        'SK_DM_MUNICIPIOS_ESTABELECIMENTO',
        col('document_id').alias('CD_VACINACAO'),
        col('vacina_dataAplicacao').alias('DT_VACINACAO')
        
    )
    .dropDuplicates(subset=['CD_VACINACAO'])
    .withColumn('SK_FT_VACINACAO', xxhash64('CD_VACINACAO'))
)

# %%
df_vacinacao.write.mode('append').format('delta').save(output_directory)

# %%
(
    df_vacinacao
    .write
    .format('bigquery')
    .option('table', f'ds_pgii.{tablename}')
    .mode('append')
    .save()
)