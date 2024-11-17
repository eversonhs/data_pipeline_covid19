# %% [markdown]
# # Tratamento da base de dados de vacinação contra COVID-19

# %% [markdown]
# ## 1.0. Configurações

# %% [markdown]
# ### 1.1. Importação das dependências necessárias

# %% [markdown]
# - Pyspark 3.4.1

# %%
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
# %% [markdown]
# 1.2. Configuração do argparse
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--year',
        help="Year of execution",
    )
    parser.add_argument(
        '--month',
        help="month of execution",
    )
    parser.add_argument(
        '--day',
        help="day of execution",
    )
    parser.add_argument(
        '--output_date',
        help="day of execution in format YYYY-MM-DD",
        required=False,
        type=lambda x: datetime.strptime(x, "%Y-%m-%d")
    )

    known_args = parser.parse_args()
    if known_args.output_date is None:
        known_args.output_date = datetime.strptime(f"{known_args.year}-{known_args.month}-{known_args.day}", "%Y-%m-%d")

    return known_args

exec_args = parse_args()
input_directory = f"gs://pgii-bronze/vacinacao_covid19/json/{{{exec_args.year}}}/{{{exec_args.month}}}/{{{exec_args.day}}}/*"
output_partition = exec_args.output_date.strftime("%Y/%m/%d")
output_directory = f"gs://pgii-silver/vacinacao_covid19/{output_partition}/"
temp_bucket = "gs://pgii-dataproc-temp"

# %% [markdown]
# ### 1.3. Configuração do contexto Spark

# %%
spark = SparkSession.Builder() \
    .appName("covid_19_vacination_data_cleaning_json") \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', temp_bucket)
# %% [markdown]
# ## 2.0. Leitura dos dados

# %%
df_vacinacao = (
    spark
    .read
    .format('json')
    .option('inferSchema', 'true')
    .option('multiLine', 'true')
    .load(input_directory)
    .select('_source.*')
)

# ## 2.1. Seleção dos campos que serão utilizados
df_vacinacao = df_vacinacao.select(
    'document_id',
    'paciente_id',
    'paciente_idade',
    'paciente_dataNascimento',
    'paciente_enumSexoBiologico',
    'paciente_racaCor_codigo',
    'paciente_racaCor_valor',
    'paciente_endereco_coIbgeMunicipio',
    'paciente_endereco_coPais',
    'paciente_endereco_nmMunicipio',
    'paciente_endereco_nmPais',
    'paciente_endereco_uf',
    'paciente_endereco_cep',
    'paciente_nacionalidade_enumNacionalidade',
    'estabelecimento_valor',
    'estabelecimento_razaoSocial',
    'estalecimento_noFantasia',
    'estabelecimento_municipio_codigo',
    'estabelecimento_municipio_nome',
    'estabelecimento_uf',
    'vacina_grupoAtendimento_codigo',
    'vacina_grupoAtendimento_nome',
    'vacina_categoria_codigo',
    'vacina_categoria_nome',
    'vacina_lote',
    'vacina_fabricante_nome',
    'vacina_fabricante_referencia',
    'vacina_dataAplicacao',
    'vacina_descricao_dose',
    'vacina_codigo',
    'vacina_nome',
    'sistema_origem'
)

# %%
# ## 3.0. Tratamento dos dados
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

df_vacinacao.write.format('parquet').mode('append').save(output_directory)