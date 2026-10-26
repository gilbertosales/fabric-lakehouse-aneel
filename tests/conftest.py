from pathlib import Path

import pytest
from pyspark.sql import SparkSession

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def spark():
    sessao = (
        SparkSession.builder.master("local[1]")
        .appName("testes-aneel-lakehouse")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .getOrCreate()
    )
    yield sessao
    sessao.stop()


@pytest.fixture(scope="session")
def bronze(spark):
    from aneel_lakehouse import bronze

    df = bronze.ler_csv_gd(spark, str(FIXTURES / "gd_amostra.csv"))
    return bronze.adicionar_metadados(df, "teste-001")


@pytest.fixture(scope="session")
def silver_tipada(bronze):
    from aneel_lakehouse import silver

    return silver.transformar(bronze)


@pytest.fixture(scope="session")
def silver_final(silver_tipada):
    from aneel_lakehouse import silver

    validos, _ = silver.separar_quarentena(silver_tipada)
    return silver.deduplicar(validos)
