# %% [markdown]
# # Tratamento da base de dados de vacinação contra COVID-19

# %% [markdown]
# ## 1.0. Configurações

# %% [markdown]
# ### 1.1. Importação das dependências necessárias

# %% [markdown]
# - Pyspark 3.4.1

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pathlib import Path
import os

# %% [markdown]
# ### 1.2. Configuração do contexto Spark

temp_bucket = "gs://pgii-dataproc-temp"
input_directory = "gs://pgii-raw/vacinacao_covid19/csv/*"
tablename = 'vacinacao_covid19'
output_directory = "gs://pgii-trusted/vacinacao_covid19/csv/*"

# %%
spark = SparkSession.builder \
    .appName(f"covid_19_vacination_{tablename}") \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', temp_bucket)

# %%
data_path = Path(os.environ["DATA_PATH"])

# %% [markdown]
# ## 2.0. Leitura dos dados

# %%
df_vacinacao = (
    spark
    .read
    .format('csv')
    .option('inferSchema', 'true')
    .option('sep', ';')
    .option('header', 'true')
    .load(input_directory)
)

# %%
# ## 3.0. Tratamento dos dados
# ### 3.1. campos textuais
df_vacinacao = (
    df_vacinacao
    .select(
        *[translate(trim(col(column)), 'ãâäöüẞáäçčďéěíĺľňóôõŕšťúůýžÄÖÜẞÁÃÂÄÇČĎÉĚÍĹĽŇÓÔÕŔŠŤÚŮÝŽṔ','aaaousaaccdeeillnooorstuuyzAOUSAAAACCDEEILLNOOORSTUUYZP').alias(column) if dict(df_vacinacao.dtypes)[column] == 'string' else col(column) for column in df_vacinacao.columns]
    )
)

# ### 3.2. Campos nulos
df_vacinacao = df_vacinacao.replace({
    'None': None,
    '': None,
    'N/A': None,
    'Pendente Identificacao': None
})

# ### 3.3. Campos específicos
df_vacinacao = (
    df_vacinacao
    .withColumn('vacina_lote', upper(col('vacina_lote')))
)

df_vacinacao = (
    df_vacinacao
    .withColumn('vacina_dataAplicacao', regexp_replace('vacina_dataAplicacao', '[^0-9]', ''))
    .withColumn('vacina_dataAplicacao', to_date('vacina_dataAplicacao', format='yyyyMMdd'))
)

df_vacinacao.write.format('parquet').mode('overwrite').save(output_directory)