"""Funcoes puras de transformacao das tabelas do pipeline NYC TLC.

Este modulo NAO depende de `dbutils`, `spark` ou `display` (globais do
Databricks): recebe e retorna `DataFrame`s, para poder ser exercitado
localmente com uma `SparkSession` em testes unitarios (pytest).

Os notebooks (`src/bronze/template.py`, `src/silver_trips.py`,
`src/raw_ingestion.py`) importam estas funcoes, garantindo que os testes
validem exatamente a mesma logica que roda em producao.
"""

import datetime
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Colunas obrigatorias na camada de consumo (exigencia do case).
REQUIRED_COLUMNS = [
    "VendorID",
    "passenger_count",
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]

# Nomes possiveis da coluna de pickup, por tipo de taxi.
PICKUP_COL_CANDIDATES = ["tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_datetime"]


def month_list(date_start: str, date_stop: str) -> list:
    """Lista de meses (YYYY-MM) do primeiro dia de cada mes no intervalo."""
    start = datetime.datetime.strptime(date_start, "%Y-%m-%d").replace(day=1)
    stop = datetime.datetime.strptime(date_stop, "%Y-%m-%d").replace(day=1)

    months, current = [], start
    while current <= stop:
        months.append(current.strftime("%Y-%m"))
        # avanca com seguranca para o primeiro dia do mes seguinte
        current = (current.replace(day=28) + datetime.timedelta(days=7)).replace(day=1)
    return months


def detect_pickup_col(columns, candidates=None):
    """Retorna a primeira coluna de pickup encontrada, ou None."""
    candidates = candidates or PICKUP_COL_CANDIDATES
    return next((c for c in candidates if c in columns), None)


def deduplicate(df: DataFrame) -> DataFrame:
    """Remove linhas 100% identicas (todas as colunas)."""
    return df.dropDuplicates(df.columns)


# Ordem de "largura" dos tipos numericos para promocao (menor -> maior).
_NUMERIC_RANK = {
    T.ByteType: 1,
    T.ShortType: 2,
    T.IntegerType: 3,
    T.LongType: 4,
    T.FloatType: 5,
    T.DoubleType: 6,
}
_NUMERIC_LIKE = (
    T.ByteType, T.ShortType, T.IntegerType, T.LongType,
    T.FloatType, T.DoubleType, T.DecimalType,
)


def _promote_type(types):
    """Resolve um unico tipo-alvo para uma coluna presente em varios arquivos.

    - Tipos identicos -> o proprio tipo.
    - Numeros compativeis -> o mais "largo" (ex.: int + double -> double).
    - Numeros envolvendo decimal -> double.
    - Qualquer outra divergencia -> string (rede de seguranca sem perda textual).
    """
    distinct = {t.simpleString(): t for t in types}
    if len(distinct) == 1:
        return next(iter(distinct.values()))
    if all(type(t) in _NUMERIC_RANK for t in types):
        return max(types, key=lambda t: _NUMERIC_RANK[type(t)])
    if all(isinstance(t, _NUMERIC_LIKE) for t in types):
        return T.DoubleType()
    return T.StringType()


def unify_schemas(dfs) -> DataFrame:
    """Une varios DataFrames reconciliando divergencias de schema.

    A TLC muda o schema entre meses de duas formas que quebram o
    `spark.read.option("mergeSchema", ...)`:
      1. mesma coluna com tipos diferentes (ex.: int vs double);
      2. mesma coluna com grafias diferentes de caixa (ex.: `airport_fee` vs
         `Airport_fee`), que colidem porque o Spark e case-insensitive por padrao.

    Esta funcao le cada arquivo separadamente e alinha as colunas de forma
    case-insensitive: usa a primeira grafia vista como canonica, promove o
    tipo-alvo (numericos -> mais largo) e preenche colunas ausentes com NULL.
    """
    if not dfs:
        raise ValueError("unify_schemas requer ao menos um DataFrame")

    order = []            # chaves (lower) na ordem de primeira aparicao
    canon_name = {}       # lower -> grafia canonica (primeira vista)
    types_by_key = {}     # lower -> lista de tipos observados
    for df in dfs:
        for field in df.schema.fields:
            key = field.name.lower()
            if key not in canon_name:
                canon_name[key] = field.name
                order.append(key)
                types_by_key[key] = []
            types_by_key[key].append(field.dataType)

    target = {key: _promote_type(types_by_key[key]) for key in order}

    aligned = []
    for df in dfs:
        # Mapeia cada coluna deste df pela chave (lower); primeira vence caso
        # o arquivo tenha as duas grafias.
        actual_by_key = {}
        for field in df.schema.fields:
            actual_by_key.setdefault(field.name.lower(), field.name)

        cols = []
        for key in order:
            alias = canon_name[key]
            if key in actual_by_key:
                cols.append(F.col(actual_by_key[key]).cast(target[key]).alias(alias))
            else:
                cols.append(F.lit(None).cast(target[key]).alias(alias))
        aligned.append(df.select(*cols))

    return reduce(lambda a, b: a.unionByName(b), aligned)


def build_bronze(df: DataFrame) -> DataFrame:
    """Bronze = replica exata: dedup + metadado de ingestao + particoes.

    Preserva todas as colunas originais; adiciona apenas `dt_ingestion` e,
    quando ha coluna de pickup, `pickup_year`/`pickup_month`.
    """
    df = deduplicate(df)

    pickup_col = detect_pickup_col(df.columns)
    df = df.withColumn("dt_ingestion", F.current_timestamp())

    if pickup_col is not None:
        df = (
            df.withColumn("pickup_year", F.year(F.col(pickup_col).cast("timestamp")))
            .withColumn("pickup_month", F.month(F.col(pickup_col).cast("timestamp")))
        )
    return df


def build_silver(df: DataFrame, year: int = 2023, month_start: int = 1, month_stop: int = 5) -> DataFrame:
    """Silver = camada de consumo: seleciona obrigatorias, tipa, limpa, particiona."""
    df = (
        df.select(*REQUIRED_COLUMNS)
        .withColumn("VendorID", F.col("VendorID").cast("int"))
        .withColumn("passenger_count", F.col("passenger_count").cast("int"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast("timestamp"))
        .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast("timestamp"))
        .withColumn("pickup_year", F.year("tpep_pickup_datetime"))
        .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
    )

    df = df.filter(
        F.col("tpep_pickup_datetime").isNotNull()
        & F.col("tpep_dropoff_datetime").isNotNull()
        & (F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime"))
        & (F.col("total_amount") >= 0)
        & (F.col("pickup_year") == year)
        & (F.col("pickup_month").between(month_start, month_stop))
    )
    return df
