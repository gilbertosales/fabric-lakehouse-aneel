# %% [markdown]
# # 02 · Silver: tipagem, LGPD, qualidade e deduplicação
# - converte números no formato brasileiro e datas
# - **remove CPF/CNPJ e nome do titular** (não saem da bronze)
# - registros que violam regras vão para a tabela de quarentena com o motivo
# - `MERGE` na silver mantém a versão mais recente de cada empreendimento

# %% tags=["parameters"]
data_carga = ""

# %%
from delta.tables import DeltaTable

from aneel_lakehouse import silver
from aneel_lakehouse.schemas import TABELA_BRONZE, TABELA_QUARENTENA, TABELA_SILVER

bronze_df = spark.table(TABELA_BRONZE).filter(f"_data_carga = '{data_carga}'")
tipada = silver.transformar(bronze_df)
silver.garantir_sem_pii(tipada)

validos, quarentena = silver.separar_quarentena(tipada)
validos = silver.deduplicar(validos)

quarentena.write.format("delta").mode("append").saveAsTable(TABELA_QUARENTENA)

# %%
if spark.catalog.tableExists(TABELA_SILVER):
    (
        DeltaTable.forName(spark, TABELA_SILVER)
        .alias("s")
        .merge(validos.alias("n"), "s.cod_empreendimento = n.cod_empreendimento")
        .whenMatchedUpdateAll(condition="n.data_conjunto_dados >= s.data_conjunto_dados")
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    validos.write.format("delta").saveAsTable(TABELA_SILVER)

spark.sql(f"OPTIMIZE {TABELA_SILVER} ZORDER BY (uf, data_conexao)")

# %%
from aneel_lakehouse import quality

taxa = quality.taxa_quarentena(validos.count(), quarentena.count())
print(taxa.detalhe)
quality.falhar_se_reprovado([taxa])
