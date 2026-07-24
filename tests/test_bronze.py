"""Testes da tabela bronze (`nyc_taxi.bronze.<taxi_type>_trips`).

Bronze = replica exata: dedup de linhas identicas, preserva todas as colunas
originais e adiciona metadado de ingestao + colunas de particao.
"""

from pyspark.sql import types as T

from transforms import build_bronze, unify_schemas


def test_bronze_removes_identical_rows(spark):
    rows = [
        (1, "2023-01-01 00:00:00"),
        (1, "2023-01-01 00:00:00"),  # duplicata exata
        (2, "2023-02-01 00:00:00"),
    ]
    df = spark.createDataFrame(rows, ["VendorID", "tpep_pickup_datetime"])

    out = build_bronze(df)

    assert out.count() == 2


def test_bronze_dedup_off_keeps_duplicates(spark):
    rows = [
        (1, "2023-01-01 00:00:00"),
        (1, "2023-01-01 00:00:00"),  # duplicata exata - deve ser MANTIDA
        (2, "2023-02-01 00:00:00"),
    ]
    df = spark.createDataFrame(rows, ["VendorID", "tpep_pickup_datetime"])

    out = build_bronze(df, dedup=False)

    assert out.count() == 3


def test_bronze_dedup_by_key_columns(spark):
    # Mesma chave (pickup + PU) em duas linhas com valores diferentes de fare:
    # dedup por chave deve manter apenas uma.
    rows = [
        (1, "2023-01-01 00:00:00", 132, 10.0),
        (2, "2023-01-01 00:00:00", 132, 99.0),  # mesma chave (pickup, PU)
        (3, "2023-01-02 00:00:00", 100, 20.0),
    ]
    cols = ["VendorID", "tpep_pickup_datetime", "PULocationID", "fare_amount"]
    df = spark.createDataFrame(rows, cols)

    out = build_bronze(df, dedup_keys=["tpep_pickup_datetime", "PULocationID"])

    assert out.count() == 2


def test_bronze_dedup_keys_absent_falls_back_to_all_columns(spark):
    rows = [
        (1, "2023-01-01 00:00:00"),
        (1, "2023-01-01 00:00:00"),  # duplicata exata
        (2, "2023-02-01 00:00:00"),
    ]
    df = spark.createDataFrame(rows, ["VendorID", "tpep_pickup_datetime"])

    # chave inexistente -> fallback para todas as colunas (remove a duplicata)
    out = build_bronze(df, dedup_keys=["coluna_inexistente"])

    assert out.count() == 2


def test_bronze_preserves_all_columns_and_adds_metadata(spark):
    df = spark.createDataFrame(
        [(1, "2023-01-15 08:30:00", "algum_valor")],
        ["VendorID", "tpep_pickup_datetime", "extra_col"],
    )

    out = build_bronze(df)

    for col in ["VendorID", "tpep_pickup_datetime", "extra_col", "dt_ingestion", "pickup_year", "pickup_month"]:
        assert col in out.columns, f"coluna ausente: {col}"


def test_bronze_derives_partition_values(spark):
    df = spark.createDataFrame(
        [(1, "2023-04-10 12:00:00")],
        ["VendorID", "tpep_pickup_datetime"],
    )

    row = build_bronze(df).collect()[0]

    assert row["pickup_year"] == 2023
    assert row["pickup_month"] == 4


def test_bronze_without_pickup_column(spark):
    df = spark.createDataFrame([(1,), (2,)], ["VendorID"])

    out = build_bronze(df)

    assert "dt_ingestion" in out.columns
    assert "pickup_year" not in out.columns
    assert "pickup_month" not in out.columns


def test_bronze_supports_green_pickup_column(spark):
    df = spark.createDataFrame(
        [(1, "2023-05-01 00:00:00")],
        ["VendorID", "lpep_pickup_datetime"],
    )

    row = build_bronze(df).collect()[0]

    assert row["pickup_year"] == 2023
    assert row["pickup_month"] == 5


def test_unify_schemas_promotes_conflicting_numeric_types(spark):
    # Mesmo cenario da TLC: 'airport_fee' vem como long em um mes e double no outro.
    schema_a = T.StructType([
        T.StructField("VendorID", T.LongType()),
        T.StructField("airport_fee", T.LongType()),
    ])
    schema_b = T.StructType([
        T.StructField("VendorID", T.LongType()),
        T.StructField("airport_fee", T.DoubleType()),
    ])
    df_a = spark.createDataFrame([(1, 0)], schema_a)
    df_b = spark.createDataFrame([(2, 1.75)], schema_b)

    out = unify_schemas([df_a, df_b])

    assert out.count() == 2
    assert dict(out.dtypes)["airport_fee"] == "double"


def test_unify_schemas_fills_missing_columns_with_null(spark):
    # Coluna 'congestion_surcharge' so existe no segundo arquivo.
    df_a = spark.createDataFrame([(1,)], ["VendorID"])
    df_b = spark.createDataFrame([(2, 2.5)], ["VendorID", "congestion_surcharge"])

    out = unify_schemas([df_a, df_b])

    assert set(out.columns) == {"VendorID", "congestion_surcharge"}
    values = {r["VendorID"]: r["congestion_surcharge"] for r in out.collect()}
    assert values[1] is None
    assert values[2] == 2.5


def test_unify_schemas_merges_case_variant_columns(spark):
    # A TLC ora escreve 'airport_fee', ora 'Airport_fee': devem virar UMA coluna.
    df_a = spark.createDataFrame([(1, 1.25)], ["VendorID", "airport_fee"])
    df_b = spark.createDataFrame([(2, 1.75)], ["VendorID", "Airport_fee"])

    out = unify_schemas([df_a, df_b])

    # So uma coluna de airport fee (na grafia canonica, a primeira vista).
    airport_cols = [c for c in out.columns if c.lower() == "airport_fee"]
    assert airport_cols == ["airport_fee"]
    assert out.count() == 2
    values = {r["VendorID"]: r["airport_fee"] for r in out.collect()}
    assert values[1] == 1.25
    assert values[2] == 1.75
