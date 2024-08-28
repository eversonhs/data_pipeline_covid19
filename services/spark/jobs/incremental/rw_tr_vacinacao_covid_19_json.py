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
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

# %% [markdown]
# 1.2. Configuração do argparse
parser = argparse.ArgumentParser()
parser.add_argument(
    '--date',
    help="Execution date in the format YYYY-MM-DD",
    default=date.today().strftime('%Y-%m-%d')
)
args = parser.parse_args()

# %% [markdown]
# ### 1.3. Configuração do contexto Spark

# %%
spark = SparkSession.Builder() \
    .master(os.environ["SPARK_MASTER_URI"]) \
    .appName("covid_19_vacination_data_cleaning_json") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()

# %%
run_date = datetime.strptime(args.date, '%Y-%m-%d')
input_directory = "/".join([
    os.environ["DATA_PATH"],
    f"raw/vacinacao_covid_19/{run_date.year:4d}/{run_date.month:02d}/{run_date.day:02d}/*"
])
# %% [markdown]
# ## 2.0. Leitura dos dados

# %%
df_vacinacao = (
    spark
    .read
    .format('json')
    .option('inferSchema', 'true')
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

# %%
output_directory = "/".join([
    os.environ["DATA_PATH"],
    f'trusted/vacinacao_covid19/{run_date.year:4d}/{run_date.month:02d}/{run_date.day:02d}'
])

df_vacinacao.write.format('parquet').mode('overwrite').save(output_directory)