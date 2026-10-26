"""Checagens de qualidade executadas ao fim de cada carga e gravadas em tabela de log."""

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass
class Resultado:
    checagem: str
    tabela: str
    aprovado: bool
    detalhe: str


def nao_vazio(df: DataFrame, tabela: str) -> Resultado:
    n = df.count()
    return Resultado("nao_vazio", tabela, n > 0, f"{n} linhas")


def chave_unica(df: DataFrame, tabela: str, chave: str) -> Resultado:
    duplicadas = df.groupBy(chave).count().filter("count > 1").count()
    return Resultado("chave_unica", tabela, duplicadas == 0, f"{duplicadas} chaves duplicadas em {chave}")


def integridade_referencial(fato: DataFrame, dim: DataFrame, chave: str, tabela: str) -> Resultado:
    orfaos = fato.join(dim, on=chave, how="left_anti").count()
    return Resultado("integridade_referencial", tabela, orfaos == 0, f"{orfaos} linhas sem {chave} na dimensão")


def taxa_quarentena(validos: int, rejeitados: int, limite: float = 0.05) -> Resultado:
    total = validos + rejeitados
    taxa = rejeitados / total if total else 0.0
    return Resultado("taxa_quarentena", "silver", taxa <= limite, f"{taxa:.2%} rejeitados (limite {limite:.0%})")


def para_dataframe(spark: SparkSession, resultados: list[Resultado], id_execucao: str) -> DataFrame:
    linhas = [(r.checagem, r.tabela, r.aprovado, r.detalhe) for r in resultados]
    return spark.createDataFrame(
        linhas, "checagem string, tabela string, aprovado boolean, detalhe string"
    ).withColumns({"id_execucao": F.lit(id_execucao), "executado_em": F.current_timestamp()})


def falhar_se_reprovado(resultados: list[Resultado]) -> None:
    reprovados = [r for r in resultados if not r.aprovado]
    if reprovados:
        raise RuntimeError(
            "Checagens reprovadas: " + "; ".join(f"{r.tabela}.{r.checagem}: {r.detalhe}" for r in reprovados)
        )
