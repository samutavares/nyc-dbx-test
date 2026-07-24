"""Testes da camada silver (uma tabela por tipo: `nyc_taxi.silver.<taxi_type>_trips`).

Silver = padronizacao leve: converte todos os nomes para snake_case, tipa as
colunas de data/hora e zona, deriva particoes e enriquece com zonas -
mantendo TODAS as colunas e TODAS as linhas (limpeza/agregacao ficam no gold).
"""

from transforms import standardize_silver

# Colunas cruas estilo yellow (nomes originais da TLC, antes do snake_case).
YELLOW_COLUMNS = [
    "VendorID",
    "passenger_count",
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
]


def _yellow_df(spark, rows):
    return spark.createDataFrame(rows, YELLOW_COLUMNS)


def _zone_df(spark):
    return spark.createDataFrame(
        [
            (132, "Queens", "JFK Airport", "Airports"),
            (100, "Manhattan", "Garment District", "Yellow Zone"),
        ],
        ["LocationID", "Borough", "Zone", "service_zone"],
    )


def test_silver_snake_cases_and_keeps_all_columns(spark):
    df = _yellow_df(spark, [(1, 2, 10.0, "2023-01-01 10:00:00", "2023-01-01 10:30:00", 132, 100)])

    out = standardize_silver(df)

    for col in out.columns:
        assert col == col.lower(), f"coluna nao snake_case: {col}"
    # renomeacoes esperadas
    assert {"vendor_id", "pu_location_id", "do_location_id"} <= set(out.columns)
    # nada descartado: colunas originais (ja snake) permanecem
    assert {"passenger_count", "total_amount", "tpep_pickup_datetime"} <= set(out.columns)


def test_silver_casts_datetime_and_location(spark):
    df = _yellow_df(spark, [("1", 2, "10.5", "2023-05-01 10:00:00", "2023-05-01 10:30:00", "132", "100")])

    dtypes = dict(standardize_silver(df).dtypes)

    assert dtypes["tpep_pickup_datetime"] == "timestamp"
    assert dtypes["tpep_dropoff_datetime"] == "timestamp"
    assert dtypes["pu_location_id"] == "int"
    assert dtypes["do_location_id"] == "int"


def test_silver_keeps_all_rows(spark):
    # Linhas que a antiga limpeza descartaria devem ser MANTIDAS no silver.
    rows = [
        (1, 1, 10.0, "2023-01-01 10:00:00", "2023-01-01 10:30:00", 1, 2),
        (1, 1, -5.0, "2023-01-01 10:00:00", "2023-01-01 10:30:00", 1, 2),   # valor negativo
        (1, 1, 10.0, "2022-12-01 10:00:00", "2022-12-01 10:30:00", 1, 2),   # fora do periodo
    ]

    out = standardize_silver(_yellow_df(spark, rows))

    assert out.count() == 3


def test_silver_derives_partition_values(spark):
    df = _yellow_df(spark, [(1, 1, 20.0, "2023-05-20 09:15:00", "2023-05-20 09:45:00", 1, 2)])

    row = standardize_silver(df).collect()[0]

    assert row["pickup_year"] == 2023
    assert row["pickup_month"] == 5


def test_silver_enriches_with_zone_lookup(spark):
    df = _yellow_df(spark, [(1, 1, 55.0, "2023-01-02 10:00:00", "2023-01-02 10:40:00", 132, 100)])

    row = standardize_silver(df, zone_df=_zone_df(spark)).collect()[0]

    assert row["pickup_borough"] == "Queens"
    assert row["pickup_zone"] == "JFK Airport"
    assert row["dropoff_borough"] == "Manhattan"
    assert row["dropoff_zone"] == "Garment District"
    assert row["is_airport_trip"] is True


def test_silver_marks_non_airport_trip(spark):
    df = _yellow_df(spark, [(1, 1, 12.0, "2023-01-02 10:00:00", "2023-01-02 10:20:00", 100, 100)])

    row = standardize_silver(df, zone_df=_zone_df(spark)).collect()[0]

    assert row["is_airport_trip"] is False


def test_silver_left_join_keeps_unknown_zone(spark):
    df = _yellow_df(spark, [(1, 1, 12.0, "2023-01-02 10:00:00", "2023-01-02 10:20:00", 999, 100)])

    out = standardize_silver(df, zone_df=_zone_df(spark))
    row = out.collect()[0]

    assert out.count() == 1
    assert row["pickup_borough"] is None
    assert row["dropoff_borough"] == "Manhattan"


def test_silver_fhv_columns_snake_cased_and_typed(spark):
    cols = ["dispatching_base_num", "pickup_datetime", "dropOff_datetime", "PUlocationID", "DOlocationID", "SR_Flag"]
    df = spark.createDataFrame(
        [("B1", "2023-01-01 10:00:00", "2023-01-01 10:20:00", 132, 100, 1)],
        cols,
    )

    out = standardize_silver(df)
    dtypes = dict(out.dtypes)

    assert {"drop_off_datetime", "pu_location_id", "do_location_id", "sr_flag"} <= set(out.columns)
    assert dtypes["pickup_datetime"] == "timestamp"
    assert dtypes["drop_off_datetime"] == "timestamp"
    assert dtypes["pu_location_id"] == "int"
