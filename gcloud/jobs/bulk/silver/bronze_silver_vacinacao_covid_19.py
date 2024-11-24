# %% [markdown]
# # Tratamento da base de dados de vacinação contra COVID-19

# %% [markdown]
# ## 1.0. Configurações

# %% [markdown]
# ### 1.1. Importação das dependências necessárias

# %% [markdown]
# - Pyspark 3.4.1

# %%
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, UTC
import argparse

def parse_args():
    today = datetime.now(UTC)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output_date',
        help="day of execution in format YYYY-MM-DD",
        required=False,
        default=today,
        type=lambda x: datetime.strptime(x, "%Y-%m-%d")
    )

    known_args = parser.parse_args()

    return known_args


# %% [markdown]
# ### 1.2. Configuração do contexto Spark

exec_args = parse_args()
temp_bucket = "gs://pgii-dataproc-temp"
input_directory = "gs://pgii-bronze/vacinacao_covid19/csv/*"
tablename = 'vacinacao_covid19'
output_partition = exec_args.output_date.strftime("%Y/%m/%d")
output_directory = f"gs://pgii-silver/vacinacao_covid19/{output_partition}/"

# %%
spark = SparkSession.builder \
    .appName(f"covid_19_vacination_{tablename}") \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', temp_bucket)


# %% [markdown]
# ## 2.0. Leitura dos dados

# %%
schema = StructType([
    StructField("document_id", StringType()),
    StructField("paciente_id", StringType()),
    StructField("paciente_idade", IntegerType()),
    StructField("paciente_dataNascimento", DateType()),
    StructField("paciente_enumSexoBiologico", StringType()),
    StructField("paciente_racaCor_codigo", StringType()),
    StructField("paciente_racaCor_valor", StringType()),
    StructField("paciente_endereco_coIbgeMunicipio", StringType()),
    StructField("paciente_endereco_coPais", StringType()),
    StructField("paciente_endereco_nmMunicipio", StringType()),
    StructField("paciente_endereco_nmPais", StringType()),
    StructField("paciente_endereco_uf", StringType()),
    StructField("paciente_endereco_cep", StringType()),
    StructField("paciente_nacionalidade_enumNacionalidade", StringType()),
    StructField("estabelecimento_valor", IntegerType()),
    StructField("estabelecimento_razaoSocial", StringType()),
    StructField("estalecimento_noFantasia", StringType()),
    StructField("estabelecimento_municipio_codigo", IntegerType()),
    StructField("estabelecimento_municipio_nome", StringType()),
    StructField("estabelecimento_uf", StringType()),
    StructField("vacina_grupoAtendimento_codigo", IntegerType()),
    StructField("vacina_grupoAtendimento_nome", StringType()),
    StructField("vacina_categoria_codigo", IntegerType()),
    StructField("vacina_categoria_nome", StringType()),
    StructField("vacina_lote", StringType()),
    StructField("vacina_fabricante_nome", StringType()),
    StructField("vacina_fabricante_referencia", StringType()),
    StructField("vacina_dataAplicacao", StringType()),
    StructField("vacina_descricao_dose", StringType()),
    StructField("vacina_codigo", IntegerType()),
    StructField("vacina_nome", StringType()),
    StructField("sistema_origem", StringType())
])

df_vacinacao = (
    spark
    .read
    .format('csv')
    .option('sep', ';')
    .option('header', 'true')
    .schema(schema)
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