"""Camada gold: modelo estrela pronto para o Direct Lake do Power BI."""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def _dimensao(df: DataFrame, chave: str, colunas: list[str]) -> DataFrame:
    """Cria dimensão com chave substituta estável (hash das colunas naturais)."""
    return (
        df.select(*colunas)
        .dropDuplicates(colunas)
        .withColumn(chave, F.xxhash64(*[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in colunas]))
        .select(chave, *colunas)
    )


def dim_localidade(silver: DataFrame) -> DataFrame:
    return _dimensao(silver, "sk_localidade", ["cod_municipio_ibge", "municipio", "uf", "regiao"])


def dim_fonte(silver: DataFrame) -> DataFrame:
    return _dimensao(silver, "sk_fonte", ["tipo_geracao", "fonte_geracao", "porte"])


def dim_classe_consumo(silver: DataFrame) -> DataFrame:
    return _dimensao(silver, "sk_classe", ["classe_consumo", "subgrupo_tarifario", "tipo_consumidor", "modalidade"])


def dim_distribuidora(silver: DataFrame) -> DataFrame:
    return _dimensao(silver, "sk_distribuidora", ["cnpj_distribuidora", "sigla_distribuidora", "nome_distribuidora"])


def dim_calendario(spark: SparkSession, inicio: str, fim: str) -> DataFrame:
    """Calendário diário contínuo entre `inicio` e `fim` (yyyy-MM-dd)."""
    return (
        spark.sql(f"SELECT explode(sequence(to_date('{inicio}'), to_date('{fim}'), interval 1 day)) AS data")
        .withColumn("sk_data", F.date_format("data", "yyyyMMdd").cast("int"))
        .withColumn("ano", F.year("data"))
        .withColumn("trimestre", F.quarter("data"))
        .withColumn("mes", F.month("data"))
        .withColumn("ano_mes", F.date_format("data", "yyyy-MM"))
        .select("sk_data", "data", "ano", "trimestre", "mes", "ano_mes")
    )


def fato_geracao(silver: DataFrame) -> DataFrame:
    """Fato na granularidade de empreendimento, ligada às dimensões pelas mesmas chaves hash."""
    dims = {
        "sk_localidade": dim_localidade(silver),
        "sk_fonte": dim_fonte(silver),
        "sk_classe": dim_classe_consumo(silver),
        "sk_distribuidora": dim_distribuidora(silver),
    }
    fato = silver
    for chave, dim in dims.items():
        naturais = [c for c in dim.columns if c != chave]
        fato = fato.join(dim, on=[fato[c].eqNullSafe(dim[c]) for c in naturais], how="left").drop(
            *[dim[c] for c in naturais]
        )
    return fato.select(
        "cod_empreendimento",
        F.date_format("data_conexao", "yyyyMMdd").cast("int").alias("sk_data_conexao"),
        *dims.keys(),
        "potencia_instalada_kw",
        "qtd_uc_recebe_credito",
        "latitude",
        "longitude",
    )


def agg_potencia_uf_mes(silver: DataFrame) -> DataFrame:
    """Agregado para consultas rápidas: potência conectada por UF e mês, com acumulado."""
    mensal = silver.groupBy("uf", F.date_format("data_conexao", "yyyy-MM").alias("ano_mes")).agg(
        F.count("*").alias("qtd_empreendimentos"),
        F.round(F.sum("potencia_instalada_kw") / 1000, 4).alias("potencia_mw"),
    )
    janela = Window.partitionBy("uf").orderBy("ano_mes").rowsBetween(Window.unboundedPreceding, 0)
    return mensal.withColumn("potencia_mw_acumulada", F.round(F.sum("potencia_mw").over(janela), 4))
