# %% [markdown]
# # 01 · Bronze: CSV bruto → Delta
# Lê o arquivo da landing com esquema fixo (tudo string) e grava em `bronze_geracao_distribuida`,
# acrescentando colunas de linhagem. Sobrescreve apenas a partição da data da carga.

# %% tags=["parameters"]
data_carga = ""
id_execucao = ""

# %%
from pyspark.sql import functions as F

from aneel_lakehouse import bronze
from aneel_lakehouse.schemas import TABELA_BRONZE

caminho = f"Files/landing/geracao_distribuida/data_carga={data_carga}/gd.csv"
df = bronze.adicionar_metadados(bronze.ler_csv_gd(spark, caminho), id_execucao)
df = df.withColumn("_data_carga", F.lit(data_carga).cast("date"))

(
    df.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"_data_carga = '{data_carga}'")
    .partitionBy("_data_carga")
    .saveAsTable(TABELA_BRONZE)
)
print(f"{df.count():,} linhas gravadas em {TABELA_BRONZE}")
