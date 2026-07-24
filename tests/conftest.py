"""Fixtures compartilhadas para os testes unitarios.

Adiciona `src/lib` ao sys.path para importar o modulo `transforms` e cria uma
SparkSession local (reutilizada por toda a sessao de testes).
"""

import os
import sys

import pytest

SRC_LIB = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src", "lib"))
if SRC_LIB not in sys.path:
    sys.path.insert(0, SRC_LIB)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("nyc_taxi_tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
