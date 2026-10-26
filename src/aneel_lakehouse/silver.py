"""Camada silver: tipagem, padronização, remoção de PII, deduplicação e quarentena."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .schemas import COLUNAS_PII


def _decimal_br(coluna: str) -> F.Column:
    """Converte número no formato brasileiro ("1.234,56") para double."""
    limpo = F.regexp_replace(F.regexp_replace(F.trim(F.col(coluna)), r"\.", ""), ",", ".")
    return F.when(limpo == "", None).otherwise(limpo).cast("double")


def _texto(coluna: str) -> F.Column:
    """Remove espaços e transforma string vazia em nulo."""
    t = F.trim(F.col(coluna))
    return F.when(t == "", None).otherwise(t)


def transformar(df_bronze: DataFrame) -> DataFrame:
    """Aplica tipagem e padronização. Não filtra registros: isso é papel de `separar_quarentena`."""
    return df_bronze.select(
        _texto("CodEmpreendimento").alias("cod_empreendimento"),
        F.to_date(_texto("DatGeracaoConjuntoDados")).alias("data_conjunto_dados"),
        F.to_date(_texto("DthAtualizaCadastralEmpreend")).alias("data_conexao"),
        F.regexp_replace(_texto("NumCNPJDistribuidora"), r"\D", "").alias("cnpj_distribuidora"),
        F.upper(_texto("SigAgente")).alias("sigla_distribuidora"),
        _texto("NomAgente").alias("nome_distribuidora"),
        _texto("DscClasseConsumo").alias("classe_consumo"),
        _texto("DscSubGrupoTarifario").alias("subgrupo_tarifario"),
        F.upper(_texto("SigUF")).alias("uf"),
        _texto("NomRegiao").alias("regiao"),
        _texto("CodMunicipioIbge").alias("cod_municipio_ibge"),
        F.initcap(_texto("NomMunicipio")).alias("municipio"),
        F.upper(_texto("SigTipoConsumidor")).alias("tipo_consumidor"),
        _texto("DscModalidadeHabilitado").alias("modalidade"),
        _texto("QtdUCRecebeCredito").cast("int").alias("qtd_uc_recebe_credito"),
        F.upper(_texto("SigTipoGeracao")).alias("tipo_geracao"),
        _texto("DscFonteGeracao").alias("fonte_geracao"),
        _texto("DscPorte").alias("porte"),
        _decimal_br("NumCoordNEmpreendimento").alias("latitude"),
        _decimal_br("NumCoordEEmpreendimento").alias("longitude"),
        _decimal_br("MdaPotenciaInstaladaKW").alias("potencia_instalada_kw"),
        F.col("_id_execucao"),
        F.col("_ingerido_em"),
    )


def regras_validacao() -> dict[str, F.Column]:
    """Cada regra devolve True quando o registro é válido."""
    return {
        "chave_presente": F.col("cod_empreendimento").isNotNull(),
        "data_conexao_valida": F.col("data_conexao").isNotNull() & (F.col("data_conexao") <= F.current_date()),
        "potencia_positiva": F.col("potencia_instalada_kw") > 0,
        "uf_valida": F.length("uf") == 2,
        "coordenadas_no_brasil": F.col("latitude").isNull()
        | (F.col("latitude").between(-34.0, 6.0) & F.col("longitude").between(-74.0, -28.0)),
    }


def separar_quarentena(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Divide em (válidos, quarentena). A quarentena guarda o motivo da rejeição."""
    regras = regras_validacao()
    motivos = F.array_compact(
        F.array(*[F.when(~F.coalesce(cond, F.lit(False)), F.lit(nome)) for nome, cond in regras.items()])
    )
    marcado = df.withColumn("_motivos_rejeicao", motivos)
    validos = marcado.filter(F.size("_motivos_rejeicao") == 0).drop("_motivos_rejeicao")
    quarentena = marcado.filter(F.size("_motivos_rejeicao") > 0)
    return validos, quarentena


def deduplicar(df: DataFrame) -> DataFrame:
    """Mantém a versão mais recente de cada empreendimento."""
    janela = Window.partitionBy("cod_empreendimento").orderBy(
        F.col("data_conjunto_dados").desc_nulls_last(), F.col("_ingerido_em").desc()
    )
    return df.withColumn("_rn", F.row_number().over(janela)).filter("_rn = 1").drop("_rn")


def garantir_sem_pii(df: DataFrame) -> None:
    """Falha a execução se alguma coluna de dado pessoal chegar à silver."""
    vazadas = [c for c in COLUNAS_PII if c in df.columns]
    if vazadas:
        raise ValueError(f"Colunas com dado pessoal na silver: {vazadas}")
