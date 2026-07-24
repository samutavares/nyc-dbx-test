"""Funcoes puras de transformacao das tabelas do pipeline NYC TLC.

Este modulo NAO depende de `dbutils`, `spark` ou `display` (globais do
Databricks): recebe e retorna `DataFrame`s, para poder ser exercitado
localmente com uma `SparkSession` em testes unitarios (pytest).

Os notebooks (`src/bronze/template.py`, `src/silver/template.py`,
`src/raw_ingestion.py`) importam estas funcoes, garantindo que os testes
validem exatamente a mesma logica que roda em producao.
"""

import datetime
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from data_dictionary import (
    HVFHS_LICENSE_NAMES,
    PAYMENT_TYPE_NAMES,
    RATECODE_NAMES,
    VENDOR_NAMES,
    to_snake_case,
)

# Colunas exigidas pelo case (usadas no gold/analise, nao no silver).
REQUIRED_COLUMNS = [
    "VendorID",
    "passenger_count",
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]

# Nomes possiveis da coluna de pickup, por tipo de taxi.
PICKUP_COL_CANDIDATES = ["tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_datetime"]

# Candidatos (ja em snake_case, pois o silver primeiro converte tudo para
# snake_case) usados para detectar as colunas de data/hora e de zona e, entao,
# aplicar tipagem/particao/enriquecimento. TODAS as colunas sao mantidas.
PICKUP_DT_CANDIDATES = ["tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_datetime"]
DROPOFF_DT_CANDIDATES = [
    "tpep_dropoff_datetime", "lpep_dropoff_datetime", "dropoff_datetime", "drop_off_datetime",
]
PU_LOCATION_CANDIDATES = ["pu_location_id"]
DO_LOCATION_CANDIDATES = ["do_location_id"]


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


def deduplicate(df: DataFrame, subset=None) -> DataFrame:
    """Remove duplicatas.

    - `subset=None`: remove linhas 100% identicas (todas as colunas). Correto,
      porem caro em datasets grandes (shuffle de todas as colunas).
    - `subset=[...]`: remove duplicatas apenas pelas colunas informadas (chave
      natural). Muito mais barato. Colunas inexistentes sao ignoradas; se
      nenhuma existir, faz fallback para todas as colunas.
    """
    if subset:
        keys = [c for c in subset if c in df.columns]
        if keys:
            return df.dropDuplicates(keys)
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


def build_bronze(df: DataFrame, dedup: bool = True, dedup_keys=None) -> DataFrame:
    """Bronze = replica exata: dedup (opcional) + metadado de ingestao.

    Preserva todas as colunas originais e adiciona apenas `dt_ingestion`. O
    bronze NAO e particionado (o particionamento por mes explodia em diretorios
    orfaos por causa de datas sujas da TLC); a limpeza de datas e a derivacao de
    particao ficam no silver.

    Parametros de dedup (a raw e idempotente e o bronze faz overwrite, entao o
    dedup e apenas uma rede de seguranca):
      - `dedup=False`: nao deduplica (mais rapido; ideal para fhv/fhvhv gigantes).
      - `dedup_keys=[...]`: deduplica por uma/duas colunas-chave (barato).
      - `dedup=True` e `dedup_keys=None`: deduplica por todas as colunas (caro).
    """
    if dedup:
        df = deduplicate(df, subset=dedup_keys)

    return df.withColumn("dt_ingestion", F.current_timestamp())


def _first_present(columns, candidates):
    """Primeiro nome de `candidates` presente em `columns`, ou None."""
    return next((c for c in candidates if c in columns), None)


def snake_case_columns(df: DataFrame) -> DataFrame:
    """Renomeia TODAS as colunas do DataFrame para snake_case (sem descartar nada)."""
    for col in df.columns:
        new = to_snake_case(col)
        if new != col:
            df = df.withColumnRenamed(col, new)
    return df


def filter_valid_dates(df: DataFrame, valid_start: str, valid_end: str) -> DataFrame:
    """Remove linhas com datas invalidas (limpeza de qualidade no silver).

    Os arquivos da TLC contem `pickup_datetime` sujos (anos absurdos como 2001,
    2008, 2098, ...). Isso: (a) polui a analise da gold; e (b) explode o
    particionamento por mes em dezenas de diretorios orfaos. Aqui mantemos
    apenas as corridas com pickup dentro de `[valid_start, valid_end)` e, quando
    ha dropoff, com dropoff >= pickup (corrida coerente).

    `valid_start`/`valid_end` sao strings 'YYYY-MM-DD' (fim exclusivo).
    """
    pu_dt = _first_present(df.columns, PICKUP_DT_CANDIDATES)
    if pu_dt is None:
        return df

    pu_ts = F.col(pu_dt).cast("timestamp")
    cond = (
        pu_ts.isNotNull()
        & (pu_ts >= F.lit(valid_start).cast("timestamp"))
        & (pu_ts < F.lit(valid_end).cast("timestamp"))
    )

    do_dt = _first_present(df.columns, DROPOFF_DT_CANDIDATES)
    if do_dt is not None:
        do_ts = F.col(do_dt).cast("timestamp")
        cond = cond & (do_ts.isNull() | (do_ts >= pu_ts))

    return df.filter(cond)


def standardize_silver(
    df: DataFrame,
    zone_df: DataFrame = None,
    taxi_type: str = None,
    valid_start: str = None,
    valid_end: str = None,
) -> DataFrame:
    """Silver = padronizacao leve + limpeza de datas, mantendo TODAS as colunas.

    Transformacoes leves (mantem todas as colunas; a unica remocao de linhas e a
    limpeza de datas invalidas - selecoes/agregacoes de negocio ficam no gold):
      - converte TODOS os nomes de coluna para snake_case;
      - tipa (cast) as colunas de data/hora (timestamp) e de zona (int);
      - se `valid_start`/`valid_end` forem informados, remove datas invalidas
        (pickup fora do intervalo ou dropoff < pickup) via filter_valid_dates;
      - deriva `pickup_year`/`pickup_month` (particao) a partir do pickup;
      - se `zone_df` for fornecido, enriquece com borough/zona (colunas extras);
      - se `taxi_type` for fornecido, aplica os rotulos de negocio (colunas
        *_name a partir dos codigos e flags Y/N convertidas para boolean).
    """
    df = snake_case_columns(df)

    pu_dt = _first_present(df.columns, PICKUP_DT_CANDIDATES)
    do_dt = _first_present(df.columns, DROPOFF_DT_CANDIDATES)
    if pu_dt:
        df = df.withColumn(pu_dt, F.col(pu_dt).cast("timestamp"))
    if do_dt:
        df = df.withColumn(do_dt, F.col(do_dt).cast("timestamp"))
    for loc in PU_LOCATION_CANDIDATES + DO_LOCATION_CANDIDATES:
        if loc in df.columns:
            df = df.withColumn(loc, F.col(loc).cast("int"))

    if valid_start and valid_end:
        df = filter_valid_dates(df, valid_start, valid_end)

    if pu_dt:
        df = (
            df.withColumn("pickup_year", F.year(F.col(pu_dt)))
            .withColumn("pickup_month", F.month(F.col(pu_dt)))
        )

    if zone_df is not None:
        df = enrich_with_zones(df, zone_df)

    if taxi_type:
        df = add_coded_labels(df, taxi_type)
    return df


def _labeled_column(source_col: str, mapping: dict):
    """Expressao que traduz `source_col` para o rotulo de `mapping` (else NULL)."""
    expr = None
    for key, label in mapping.items():
        cond = F.col(source_col) == F.lit(key)
        expr = F.when(cond, F.lit(label)) if expr is None else expr.when(cond, F.lit(label))
    if expr is None:
        return F.lit(None).cast("string")
    return expr.otherwise(F.lit(None).cast("string"))


def _yn_to_bool(source_col: str):
    """Converte uma flag Y/N (string) para boolean (Y->true, N->false, else NULL)."""
    norm = F.upper(F.trim(F.col(source_col).cast("string")))
    return (
        F.when(norm == "Y", F.lit(True))
        .when(norm == "N", F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def add_coded_labels(df: DataFrame, taxi_type: str) -> DataFrame:
    """Adiciona colunas de negocio no silver, por tipo de taxi:

    - yellow/green: `vendor_name`, `ratecode_name`, `payment_type_name` a partir
      dos codigos; `store_and_fwd_flag` convertida para boolean;
    - fhvhv: `hvfhs_license_name` a partir de `hvfhs_license_num`;
      `shared_request_flag` convertida para boolean.
    """
    if taxi_type in ("yellow", "green"):
        code_labels = [
            ("vendor_id", "vendor_name", VENDOR_NAMES),
            ("ratecode_id", "ratecode_name", RATECODE_NAMES),
            ("payment_type", "payment_type_name", PAYMENT_TYPE_NAMES),
        ]
        for source_col, new_col, mapping in code_labels:
            if source_col in df.columns:
                df = df.withColumn(new_col, _labeled_column(source_col, mapping))
        if "store_and_fwd_flag" in df.columns:
            df = df.withColumn("store_and_fwd_flag", _yn_to_bool("store_and_fwd_flag"))
    elif taxi_type == "fhvhv":
        if "hvfhs_license_num" in df.columns:
            df = df.withColumn(
                "hvfhs_license_name", _labeled_column("hvfhs_license_num", HVFHS_LICENSE_NAMES)
            )
        if "shared_request_flag" in df.columns:
            df = df.withColumn("shared_request_flag", _yn_to_bool("shared_request_flag"))
    return df


def enrich_with_zones(df: DataFrame, zone_df: DataFrame) -> DataFrame:
    """Junta os IDs de zona (pu/do) com taxi_zone_lookup para trazer borough/zona.

    Espera colunas ja em snake_case (`pu_location_id`/`do_location_id` no trip e
    `location_id`/`borough`/`zone`/`service_zone` na dimensao - garantido por
    snake_case_columns). Usa left join para nao descartar corridas com zona
    desconhecida e deriva `is_airport_trip` (embarque OU desembarque em zona de
    aeroporto).
    """
    zone_df = snake_case_columns(zone_df)
    zones = zone_df.select(
        F.col("location_id").cast("int").alias("_loc_id"),
        F.col("borough").alias("_borough"),
        F.col("zone").alias("_zone"),
        F.col("service_zone").alias("_service_zone"),
    )

    if "pu_location_id" in df.columns:
        pu = zones.select(
            F.col("_loc_id").alias("pu_location_id"),
            F.col("_borough").alias("pickup_borough"),
            F.col("_zone").alias("pickup_zone"),
            F.col("_service_zone").alias("pickup_service_zone"),
        )
        df = df.join(pu, on="pu_location_id", how="left")

    if "do_location_id" in df.columns:
        do = zones.select(
            F.col("_loc_id").alias("do_location_id"),
            F.col("_borough").alias("dropoff_borough"),
            F.col("_zone").alias("dropoff_zone"),
            F.col("_service_zone").alias("dropoff_service_zone"),
        )
        df = df.join(do, on="do_location_id", how="left")

    has_pu_service = "pickup_service_zone" in df.columns
    has_do_service = "dropoff_service_zone" in df.columns
    if has_pu_service or has_do_service:
        pu_air = (F.col("pickup_service_zone") == "Airports") if has_pu_service else F.lit(False)
        do_air = (F.col("dropoff_service_zone") == "Airports") if has_do_service else F.lit(False)
        df = df.withColumn(
            "is_airport_trip",
            F.coalesce(pu_air, F.lit(False)) | F.coalesce(do_air, F.lit(False)),
        )
    return df


def comment_statements(full_table: str, comments: dict, existing_columns) -> list:
    """Gera comandos ALTER TABLE ... ALTER COLUMN ... COMMENT para as colunas.

    Aplica comentario apenas nas colunas que existem na tabela. Escapa aspas
    simples nas descricoes. Retorna a lista de SQLs (executados no notebook).
    """
    existing = set(existing_columns)
    statements = []
    for column, description in comments.items():
        if column not in existing:
            continue
        safe = description.replace("'", "''")
        statements.append(
            f"ALTER TABLE {full_table} ALTER COLUMN {column} COMMENT '{safe}'"
        )
    return statements
