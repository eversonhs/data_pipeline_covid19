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
from pyspark.sql.functions import col, xxhash64
from pathlib import Path
import os

# %%
tablename = 'ft_vacinacao'
data_path = Path(os.environ["DATA_PATH"])

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
df_vacinacao = (
    spark
    .read
    .format('parquet')
    .load("/".join([os.environ["DATA_PATH"], 'trusted/vacinacao_covid19']))
    .withColumnRenamed('paciente_id', 'CD_PACIENTE')
    .withColumnRenamed('vacina_grupoAtendimento_codigo', 'CD_GRUPO_ATENDIMENTO')
    .withColumnRenamed('vacina_categoria_codigo', 'CD_CATEGORIA_GRUPO_ATENDIMENTO')
    .withColumnRenamed('vacina_descricao_dose', 'DSC_DOSE')
    .withColumnRenamed('estabelecimento_valor', 'CD_ESTABELECIMENTO')
    .withColumnRenamed('paciente_endereco_cep', 'CD_CEP')
    .withColumnRenamed('paciente_endereco_coIbgeMunicipio', 'CD_MUNICIPIO_IBGE')
    .withColumnRenamed('vacina_lote', 'DSC_LOTE')
    .withColumnRenamed('vacina_codigo', 'CD_VACINA')
)
# %%
dm_pacientes = (
    spark
    .read
    .format('parquet')
    .load("/".join([os.environ["DATA_PATH"], 'refined/dm_pacientes']))
    .select(
        'CD_PACIENTE',
        'SK_DM_PACIENTES'
    )
)
# %%
dm_vacinas = (
    spark
    .read
    .format('parquet')
    .load("/".join([os.environ["DATA_PATH"], 'refined/dm_vacinas']))
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
    .format('parquet')
    .load("/".join([os.environ["DATA_PATH"], 'refined/dm_campanhas']))
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
    .format('parquet')
    .load("/".join([os.environ["DATA_PATH"], 'refined/dm_estabelecimentos']))
    .select(
        'CD_ESTABELECIMENTO',
        'SK_DM_ESTABELECIMENTOS'
    )
)
# %%
dm_municipios = (
    spark
    .read
    .format('parquet')
    .load("/".join([os.environ["DATA_PATH"], 'refined/dm_municipios']))
    .select(
        'CD_MUNICIPIO_IBGE',
        'CD_CEP',
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
    # .join(dm_municipios, on=['CD_MUNICIPIO_IBGE', 'CD_CEP'], how='left')
    .select(
        'SK_DM_PACIENTES',
        'SK_DM_VACINAS',
        'SK_DM_CAMPANHAS',
        'SK_DM_ESTABELECIMENTOS',
        # 'SK_DM_MUNICIPIOS',
        col('document_id').alias('CD_VACINACAO'),
        col('vacina_dataAplicacao').alias('DT_VACINACAO')
        
    )
    .withColumn('SK_FT_VACINACAO', xxhash64('CD_VACINACAO'))
)

# %%
df_vacinacao.write.mode('overwrite').format('parquet').save(f'./data/refined/{tablename}')

# %%
df_vacinacao.write.format('jdbc').options(
    url=f'jdbc:mysql://{os.environ["MYSQL_ADDRESS"]}/{os.environ["MYSQL_DATABASE"]}',
    dbtable=tablename,
    driver='com.mysql.cj.jdbc.Driver',
    user=os.environ["MYSQL_USER"],
    password=os.environ["MYSQL_PASSWORD"]
).mode('overwrite').save()