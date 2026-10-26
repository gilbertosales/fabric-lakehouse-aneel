import pytest

from aneel_lakehouse import silver
from aneel_lakehouse.schemas import COLUNAS_PII


def test_bronze_le_todas_as_linhas_com_acentos(bronze):
    assert bronze.count() == 8
    municipios = {r.NomMunicipio for r in bronze.select("NomMunicipio").collect()}
    assert "SÃO PAULO" in municipios
    assert {"_arquivo_origem", "_id_execucao", "_ingerido_em"} <= set(bronze.columns)


def test_silver_converte_decimal_brasileiro(silver_tipada):
    linha = silver_tipada.filter("cod_empreendimento = 'GD.MG.000.001.235'").first()
    assert linha.potencia_instalada_kw == pytest.approx(1250.0)
    assert linha.latitude == pytest.approx(-18.9186)
    assert linha.municipio == "Uberlândia"
    assert linha.cnpj_distribuidora == "33050196000188"


def test_silver_remove_dados_pessoais(silver_tipada):
    assert not set(COLUNAS_PII) & set(silver_tipada.columns)
    silver.garantir_sem_pii(silver_tipada)


def test_garantir_sem_pii_falha_quando_vaza(bronze):
    with pytest.raises(ValueError):
        silver.garantir_sem_pii(bronze)


def test_quarentena_registra_motivo(silver_tipada):
    validos, quarentena = silver.separar_quarentena(silver_tipada)
    assert validos.count() == 5
    motivos = {
        r.cod_empreendimento or "sem_chave": set(r._motivos_rejeicao)
        for r in quarentena.select("cod_empreendimento", "_motivos_rejeicao").collect()
    }
    assert motivos == {
        "GD.BA.000.000.777": {"potencia_positiva"},
        "sem_chave": {"chave_presente"},
        "GD.BA.000.000.779": {"coordenadas_no_brasil"},
    }


def test_deduplicacao_mantem_versao_mais_recente(silver_final):
    linhas = silver_final.filter("cod_empreendimento = 'GD.MG.000.001.234'").collect()
    assert len(linhas) == 1
    assert linhas[0].potencia_instalada_kw == pytest.approx(5.4)
    assert silver_final.count() == 4
