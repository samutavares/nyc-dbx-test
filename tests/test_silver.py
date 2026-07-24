"""Testes da tabela silver (`nyc_taxi.silver.trips`).

Silver = camada de consumo: seleciona as colunas obrigatorias, tipa, limpa
linhas invalidas e deriva colunas de particao.
"""

from transforms import REQUIRED_COLUMNS, build_silver

SILVER_COLUMNS = ["VendorID", "passenger_count", "total_amount", "tpep_pickup_datetime", "tpep_dropoff_datetime"]


def _make_df(spark, rows):
    return spark.createDataFrame(rows, SILVER_COLUMNS)


def test_silver_has_required_and_partition_columns(spark):
    df = _make_df(spark, [(1, 2, 10.0, "2023-01-01 10:00:00", "2023-01-01 10:30:00")])

    out = build_silver(df)

    for col in REQUIRED_COLUMNS + ["pickup_year", "pickup_month"]:
        assert col in out.columns, f"coluna ausente: {col}"


def test_silver_casts_types(spark):
    df = _make_df(spark, [("1", "2", "10.5", "2023-05-01 10:00:00", "2023-05-01 10:30:00")])

    dtypes = dict(build_silver(df).dtypes)

    assert dtypes["VendorID"] == "int"
    assert dtypes["passenger_count"] == "int"
    assert dtypes["total_amount"] == "double"
    assert dtypes["tpep_pickup_datetime"] == "timestamp"
    assert dtypes["tpep_dropoff_datetime"] == "timestamp"


def test_silver_keeps_only_valid_rows(spark):
    rows = [
        (1, 1, 10.0, "2023-01-01 10:00:00", "2023-01-01 10:30:00"),   # valida
        (1, 1, -5.0, "2023-01-01 10:00:00", "2023-01-01 10:30:00"),   # total_amount negativo
        (1, 1, 10.0, "2023-01-01 11:00:00", "2023-01-01 10:30:00"),   # dropoff < pickup
        (1, 1, 10.0, None, "2023-01-01 10:30:00"),                    # pickup nulo
        (1, 1, 10.0, "2022-12-01 10:00:00", "2022-12-01 10:30:00"),   # fora do ano
        (1, 1, 10.0, "2023-07-01 10:00:00", "2023-07-01 10:30:00"),   # fora do intervalo de meses
    ]

    out = build_silver(_make_df(spark, rows))

    assert out.count() == 1


def test_silver_partition_values(spark):
    df = _make_df(spark, [(1, 1, 20.0, "2023-05-20 09:15:00", "2023-05-20 09:45:00")])

    row = build_silver(df).collect()[0]

    assert row["pickup_year"] == 2023
    assert row["pickup_month"] == 5


def test_silver_drops_extra_columns(spark):
    rows = [(1, 1, 10.0, "2023-02-01 10:00:00", "2023-02-01 10:30:00", "lixo")]
    df = spark.createDataFrame(rows, SILVER_COLUMNS + ["coluna_extra"])

    out = build_silver(df)

    assert "coluna_extra" not in out.columns
