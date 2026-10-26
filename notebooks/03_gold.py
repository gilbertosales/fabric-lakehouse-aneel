# %% [markdown]
# # 03 · Gold: modelo estrela para Direct Lake
# Reconstrói as dimensões e a fato a partir da silver e roda as checagens de qualidade.
# Se alguma checagem reprovar, o notebook falha e o pipeline não atualiza o modelo semântico.

# %% tags=["parameters"]
id_execucao = ""

# %%
from aneel_lakehouse import gold, quality
from aneel_lakehouse import schemas as s

silver_df = spark.table(s.TABELA_SILVER)
tabelas = {
    s.GOLD_DIM_LOCAL: gold.dim_localidade(silver_df),
    s.GOLD_DIM_FONTE: gold.dim_fonte(silver_df),
    s.GOLD_DIM_CLASSE: gold.dim_classe_consumo(silver_df),
    s.GOLD_DIM_DISTRIBUIDORA: gold.dim_distribuidora(silver_df),
    s.GOLD_DIM_CALENDARIO: gold.dim_calendario(spark, "2008-01-01", "2030-12-31"),
    s.GOLD_FATO: gold.fato_geracao(silver_df),
    s.GOLD_AGG_UF_MES: gold.agg_potencia_uf_mes(silver_df),
}
for nome, df in tabelas.items():
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(nome)

# %%
fato = spark.table(s.GOLD_FATO)
resultados = [
    quality.nao_vazio(fato, s.GOLD_FATO),
    quality.chave_unica(fato, s.GOLD_FATO, "cod_empreendimento"),
    quality.integridade_referencial(fato, spark.table(s.GOLD_DIM_LOCAL), "sk_localidade", s.GOLD_FATO),
    quality.integridade_referencial(fato, spark.table(s.GOLD_DIM_FONTE), "sk_fonte", s.GOLD_FATO),
    quality.integridade_referencial(fato, spark.table(s.GOLD_DIM_CLASSE), "sk_classe", s.GOLD_FATO),
    quality.integridade_referencial(
        fato,
        spark.table(s.GOLD_DIM_DISTRIBUIDORA),
        "sk_distribuidora",
        s.GOLD_FATO,
    ),
]
quality.para_dataframe(spark, resultados, id_execucao).write.mode("append").saveAsTable("log_qualidade")
quality.falhar_se_reprovado(resultados)
