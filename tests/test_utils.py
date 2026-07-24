"""Testes das funcoes utilitarias puras (sem Spark)."""

from transforms import detect_pickup_col, month_list


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
