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

# %%
spark = SparkSession.Builder() \
    .master(os.environ["SPARK_MASTER_URI"]) \
    .appName("covid_19_vacination_data_cleaning") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()

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
    .load("/".join([os.environ["DATA_PATH"], 'part-00000-525841a6-ed1e-478f-89c5-61363266d156-c000.csv']))
)

# %%



# %%
df_vacinacao.write.format('parquet').mode('overwrite').save('./data/trusted/vacinacao_covid19')