"""Esquemas e constantes do dataset de Geração Distribuída da ANEEL."""

from pyspark.sql.types import StringType, StructField, StructType

# Colunas do CSV público "Relação de empreendimentos de Geração Distribuída".
# Na camada bronze tudo é lido como string: a tipagem acontece na silver.
COLUNAS_GD = [
    "DatGeracaoConjuntoDados",
    "AnmPeriodoReferencia",
    "NumCNPJDistribuidora",
    "SigAgente",
    "NomAgente",
    "CodClasseConsumo",
    "DscClasseConsumo",
    "CodSubGrupoTarifario",
    "DscSubGrupoTarifario",
    "codUFibge",
    "SigUF",
    "codRegiao",
    "NomRegiao",
    "CodMunicipioIbge",
    "NomMunicipio",
    "SigTipoConsumidor",
    "NumCPFCNPJ",
    "NomeTitularEmpreendimento",
    "CodEmpreendimento",
    "DthAtualizaCadastralEmpreend",
    "SigModalidadeEmpreendimento",
    "DscModalidadeHabilitado",
    "QtdUCRecebeCredito",
    "SigTipoGeracao",
    "DscFonteGeracao",
    "DscPorte",
    "NumCoordNEmpreendimento",
    "NumCoordEEmpreendimento",
    "MdaPotenciaInstaladaKW",
]

SCHEMA_BRONZE_GD = StructType([StructField(c, StringType(), True) for c in COLUNAS_GD])

# Dados pessoais que não saem da bronze (LGPD).
COLUNAS_PII = ["NumCPFCNPJ", "NomeTitularEmpreendimento"]

# Nomes das tabelas Delta no Lakehouse.
TABELA_BRONZE = "bronze_geracao_distribuida"
TABELA_SILVER = "silver_geracao_distribuida"
TABELA_QUARENTENA = "silver_geracao_distribuida_quarentena"
GOLD_FATO = "gold_fato_geracao_distribuida"
GOLD_DIM_LOCAL = "gold_dim_localidade"
GOLD_DIM_FONTE = "gold_dim_fonte"
GOLD_DIM_CLASSE = "gold_dim_classe_consumo"
GOLD_DIM_DISTRIBUIDORA = "gold_dim_distribuidora"
GOLD_DIM_CALENDARIO = "gold_dim_calendario"
GOLD_AGG_UF_MES = "gold_agg_potencia_uf_mes"
