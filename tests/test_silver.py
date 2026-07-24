"""Testes da camada silver (uma tabela por tipo: `nyc_taxi.silver.<taxi_type>_trips`).

Silver = padronizacao leve: converte todos os nomes para snake_case, tipa as
colunas de data/hora e zona, deriva particoes e enriquece com zonas - mantendo
TODAS as colunas. A unica remocao de linhas e a limpeza de datas invalidas
(quando `valid_start`/`valid_end` sao informados); agregacoes ficam no gold.
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


def test_silver_keeps_all_rows_without_date_window(spark):
    # Sem janela de datas, o silver NAO filtra linhas (comportamento padrao):
    # so a limpeza de datas remove linhas, e apenas quando valid_start/end sao dados.
    rows = [
        (1, 1, 10.0, "2023-01-01 10:00:00", "2023-01-01 10:30:00", 1, 2),
        (1, 1, -5.0, "2023-01-01 10:00:00", "2023-01-01 10:30:00", 1, 2),   # valor negativo
        (1, 1, 10.0, "2022-12-01 10:00:00", "2022-12-01 10:30:00", 1, 2),   # fora do periodo
    ]

    out = standardize_silver(_yellow_df(spark, rows))

    assert out.count() == 3


def test_silver_cleans_out_of_range_dates(spark):
    # Com janela valida, datas absurdas da TLC (anos 2001/2098) e anteriores ao
    # inicio sao removidas - so sobra a corrida dentro de [start, end).
    rows = [
        (1, 1, 10.0, "2023-03-01 10:00:00", "2023-03-01 10:30:00", 1, 2),   # dentro
        (1, 1, 10.0, "2098-01-01 10:00:00", "2098-01-01 10:30:00", 1, 2),   # ano absurdo
        (1, 1, 10.0, "2001-05-01 10:00:00", "2001-05-01 10:30:00", 1, 2),   # ano absurdo
        (1, 1, 10.0, "2022-12-31 23:00:00", "2022-12-31 23:30:00", 1, 2),   # antes do inicio
    ]

    out = standardize_silver(
        _yellow_df(spark, rows), valid_start="2023-01-01", valid_end="2023-06-01"
    )

    assert out.count() == 1
    assert out.collect()[0]["pickup_year"] == 2023


def test_silver_cleans_dropoff_before_pickup(spark):
    rows = [
        (1, 1, 10.0, "2023-03-01 10:00:00", "2023-03-01 10:30:00", 1, 2),   # ok
        (1, 1, 10.0, "2023-03-01 10:00:00", "2023-03-01 09:00:00", 1, 2),   # dropoff < pickup
    ]

    out = standardize_silver(
        _yellow_df(spark, rows), valid_start="2023-01-01", valid_end="2023-06-01"
    )

    assert out.count() == 1


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


GREEN_COLUMNS = [
    "VendorID",
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "total_amount",
]


def test_silver_green_adds_code_labels_and_bool_flag(spark):
    df = spark.createDataFrame(
        [(2, "2023-02-01 08:00:00", "2023-02-01 08:20:00", 2.0, "Y", 130, 140, 1.0, 25.0)],
        GREEN_COLUMNS,
    )

    out = standardize_silver(df, taxi_type="green")
    row = out.collect()[0]
    dtypes = dict(out.dtypes)

    assert row["vendor_name"] == "VeriFone Inc."
    assert row["ratecode_name"] == "JFK"
    assert row["payment_type_name"] == "Cartao de credito"
    assert dtypes["store_and_fwd_flag"] == "boolean"
    assert row["store_and_fwd_flag"] is True
    # colunas originais dos codigos permanecem
    assert {"vendor_id", "ratecode_id", "payment_type"} <= set(out.columns)


def test_silver_yellow_same_labels_as_green(spark):
    df = _yellow_df(spark, [(1, 3, 40.0, "2023-05-01 10:00:00", "2023-05-01 10:30:00", 132, 100)])

    row = standardize_silver(df, taxi_type="yellow").collect()[0]

    assert row["vendor_name"] == "Creative Mobile Technologies"


def test_silver_fhvhv_license_name_and_shared_bool(spark):
    cols = ["hvfhs_license_num", "pickup_datetime", "dropoff_datetime", "PULocationID", "DOLocationID", "shared_request_flag"]
    df = spark.createDataFrame(
        [("HV0003", "2023-01-01 10:00:00", "2023-01-01 10:20:00", 132, 100, "N")],
        cols,
    )

    out = standardize_silver(df, taxi_type="fhvhv")
    row = out.collect()[0]
    dtypes = dict(out.dtypes)

    assert row["hvfhs_license_name"] == "Uber"
    assert dtypes["shared_request_flag"] == "boolean"
    assert row["shared_request_flag"] is False
    assert "hvfhs_license_num" in out.columns


def test_silver_unknown_code_maps_to_null(spark):
    df = _yellow_df(spark, [(9, 1, 10.0, "2023-01-01 10:00:00", "2023-01-01 10:10:00", 1, 2)])

    row = standardize_silver(df, taxi_type="yellow").collect()[0]

    assert row["vendor_name"] is None


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
