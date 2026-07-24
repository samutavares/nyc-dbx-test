# Databricks notebook source
# DBTITLE 1,NYC TLC - Bronze (Exact Replica Template)
# MAGIC %md
# MAGIC # Bronze - Replica Exata (Template)
# MAGIC
# MAGIC Template **parametrizado** que le os arquivos originais da landing zone
# MAGIC para um dado `taxi_type` e materializa uma tabela **Delta** que e uma
# MAGIC replica exata da origem (todas as colunas preservadas, sem limpeza).
# MAGIC
# MAGIC A unica adicao e uma coluna de metadado de ingestao (`dt_ingestion`) e
# MAGIC colunas de particao derivadas. Nenhuma coluna original e descartada -
# MAGIC a selecao/limpeza acontece apenas na camada **silver**.
# MAGIC
# MAGIC Por ser um template, o mesmo notebook atende qualquer tipo de taxi
# MAGIC (`yellow`, `green`, `fhv`, `fhvhv`) apenas trocando o widget `taxi_type`.

# COMMAND ----------

# DBTITLE 1,Imports
import sys

sys.path.insert(0, "../lib")

from transforms import build_bronze, detect_pickup_col, month_list, unify_schemas

# COMMAND ----------

# DBTITLE 1,Parametros (widgets)
dbutils.widgets.text("taxi_type", "yellow", "Taxi type (yellow/green/fhv/fhvhv)")
dbutils.widgets.text("date_start", "2023-01-01", "First month (YYYY-MM-DD)")
dbutils.widgets.text("date_stop", "2023-05-01", "Last month (YYYY-MM-DD)")
dbutils.widgets.text("raw_path", "/Volumes/nyc_taxi/raw/landing", "Landing volume path")
dbutils.widgets.text("catalog", "nyc_taxi", "Unity Catalog")
dbutils.widgets.text("schema", "bronze", "Target bronze schema")

taxi_type = dbutils.widgets.get("taxi_type")
date_start = dbutils.widgets.get("date_start")
date_stop = dbutils.widgets.get("date_stop")
raw_path = dbutils.widgets.get("raw_path")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

table = f"{taxi_type}_trips"
full_table = f"{catalog}.{schema}.{table}"

print(f"taxi_type={taxi_type} | {date_start} -> {date_stop}")
print(f"destino: {full_table}")

# COMMAND ----------

# DBTITLE 1,Meses do intervalo (para montar o glob dos arquivos)
months = month_list(date_start, date_stop)
source_paths = [f"{raw_path}/{taxi_type}_tripdata_{ym}.parquet" for ym in months]
print(f"Arquivos: {source_paths}")

# COMMAND ----------

# DBTITLE 1,Leitura (replica exata - todas as colunas)
# A TLC muda o schema entre meses (colunas int vs double, colunas que
# somem/aparecem e ate a mesma coluna com caixa diferente, ex.: airport_fee
# vs Airport_fee), o que faz `mergeSchema` falhar (SQLSTATE 42KD9 / 42711).
# Por isso lemos cada arquivo isoladamente e reconciliamos os schemas em
# unify_schemas (case-insensitive): tipos numericos divergentes sao promovidos
# ao mais largo, colunas ausentes viram NULL. A logica e testada em tests/.
dfs = [spark.read.parquet(p) for p in source_paths]
df_raw = unify_schemas(dfs)

# DBTITLE 1,Bronze: dedup + metadado + particoes (logica testada em tests/)
# build_bronze faz dedup sobre TODAS as colunas originais (antes de adicionar
# metadado) e acrescenta dt_ingestion + pickup_year/pickup_month.
rows_before = df_raw.count()
df_bronze = build_bronze(df_raw)
rows_after = df_bronze.count()
print(f"Dedup: {rows_before:,} -> {rows_after:,} linhas ({rows_before - rows_after:,} duplicadas removidas)")

partition_by = ["pickup_year", "pickup_month"] if detect_pickup_col(df_raw.columns) else []
print(f"Colunas: {len(df_bronze.columns)} | particao: {partition_by}")

# COMMAND ----------

# DBTITLE 1,Criacao do catalogo e schema (Unity Catalog)
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# DBTITLE 1,Escrita da tabela Delta (bronze)
writer = (
    df_bronze.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
)

if partition_by:
    writer = writer.partitionBy(*partition_by)

writer.saveAsTable(full_table)

print(f"Tabela {full_table} criada/atualizada com {df_bronze.count():,} linhas.")

# COMMAND ----------

# DBTITLE 1,Validacao rapida
display(spark.sql(f"SELECT * FROM {full_table} LIMIT 20"))
