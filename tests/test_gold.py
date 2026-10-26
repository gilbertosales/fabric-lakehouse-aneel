import pytest

from aneel_lakehouse import gold, quality


def test_fato_tem_uma_linha_por_empreendimento(silver_final):
    fato = gold.fato_geracao(silver_final)
    assert fato.count() == silver_final.count()
    assert quality.chave_unica(fato, "fato", "cod_empreendimento").aprovado


@pytest.mark.parametrize(
    "construtor, chave",
    [
        (gold.dim_localidade, "sk_localidade"),
        (gold.dim_fonte, "sk_fonte"),
        (gold.dim_classe_consumo, "sk_classe"),
        (gold.dim_distribuidora, "sk_distribuidora"),
    ],
)
def test_dimensoes_cobrem_toda_a_fato(silver_final, construtor, chave):
    dim = construtor(silver_final)
    fato = gold.fato_geracao(silver_final)
    assert quality.chave_unica(dim, "dim", chave).aprovado
    assert quality.integridade_referencial(fato, dim, chave, "fato").aprovado


def test_calendario_cobre_datas_da_fato(spark, silver_final):
    cal = gold.dim_calendario(spark, "2020-01-01", "2026-12-31")
    fato = gold.fato_geracao(silver_final).withColumnRenamed("sk_data_conexao", "sk_data")
    assert quality.integridade_referencial(fato, cal, "sk_data", "fato").aprovado


def test_agregado_acumula_por_uf(silver_final):
    agg = {(r.uf, r.ano_mes): r for r in gold.agg_potencia_uf_mes(silver_final).collect()}
    assert agg[("SP", "2024-02")].potencia_mw == pytest.approx(0.0072)
    assert agg[("SP", "2025-07")].potencia_mw_acumulada == pytest.approx(0.3072)
    assert agg[("MG", "2024-02")].potencia_mw_acumulada == pytest.approx(1.2554)


def test_taxa_quarentena():
    assert quality.taxa_quarentena(95, 5).aprovado
    assert not quality.taxa_quarentena(90, 10).aprovado
