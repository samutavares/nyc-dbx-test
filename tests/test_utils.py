"""Testes das funcoes utilitarias puras (sem Spark)."""

from data_dictionary import to_snake_case
from transforms import comment_statements, detect_pickup_col, month_list


def test_month_list_case_window():
    assert month_list("2023-01-01", "2023-05-01") == [
        "2023-01",
        "2023-02",
        "2023-03",
        "2023-04",
        "2023-05",
    ]


def test_month_list_single_month():
    assert month_list("2023-03-05", "2023-03-28") == ["2023-03"]


def test_month_list_cross_year():
    assert month_list("2022-11-15", "2023-02-01") == [
        "2022-11",
        "2022-12",
        "2023-01",
        "2023-02",
    ]


def test_detect_pickup_col_yellow():
    assert detect_pickup_col(["VendorID", "tpep_pickup_datetime"]) == "tpep_pickup_datetime"


def test_detect_pickup_col_green():
    assert detect_pickup_col(["lpep_pickup_datetime", "x"]) == "lpep_pickup_datetime"


def test_detect_pickup_col_absent():
    assert detect_pickup_col(["a", "b", "c"]) is None


def test_comment_statements_only_for_existing_columns():
    comments = {"VendorID": "provedor", "ausente": "nao aplica"}
    stmts = comment_statements("cat.sch.tbl", comments, ["VendorID", "total_amount"])

    assert stmts == [
        "ALTER TABLE cat.sch.tbl ALTER COLUMN VendorID COMMENT 'provedor'"
    ]


def test_comment_statements_escapes_single_quotes():
    comments = {"col": "US$1,25 p/ 'aeroporto'"}
    stmts = comment_statements("t", comments, ["col"])

    assert stmts == ["ALTER TABLE t ALTER COLUMN col COMMENT 'US$1,25 p/ ''aeroporto'''"]


def test_to_snake_case_examples():
    assert to_snake_case("VendorID") == "vendor_id"
    assert to_snake_case("PULocationID") == "pu_location_id"
    assert to_snake_case("DOLocationID") == "do_location_id"
    assert to_snake_case("PUlocationID") == "pu_location_id"  # variante fhv (l minusculo)
    assert to_snake_case("DOlocationID") == "do_location_id"
    assert to_snake_case("RatecodeID") == "ratecode_id"
    assert to_snake_case("dropOff_datetime") == "drop_off_datetime"
    assert to_snake_case("SR_Flag") == "sr_flag"
    assert to_snake_case("Affiliated_base_number") == "affiliated_base_number"
    assert to_snake_case("tpep_pickup_datetime") == "tpep_pickup_datetime"
    assert to_snake_case("Airport_fee") == "airport_fee"
