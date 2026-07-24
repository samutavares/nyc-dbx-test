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

from data_dictionary import comments_for
from transforms import build_bronze, comment_statements, month_list, unify_schemas

# COMMAND ----------

# DBTITLE 1,Parametros (widgets)
dbutils.widgets.text("taxi_type", "yellow", "Taxi type (yellow/green/fhv/fhvhv)")
dbutils.widgets.text("date_start", "2023-01-01", "First month (YYYY-MM-DD)")
dbutils.widgets.text("date_stop", "2023-05-01", "Last month (YYYY-MM-DD)")
dbutils.widgets.text("raw_path", "/Volumes/nyc_taxi/raw/landing", "Landing volume path")
dbutils.widgets.text("catalog", "nyc_taxi", "Unity Catalog")
dbutils.widgets.text("schema", "bronze", "Target bronze schema")
# Dedup: como a raw e idempotente e o bronze faz overwrite, o dedup e so uma
# rede de seguranca. Em datasets gigantes (fhv/fhvhv) ele custa caro; desligue
# (dedup=false) ou informe 1-2 colunas-chave em dedup_keys (ex.:
# "pickup_datetime,pu_location_id"). Vazio + dedup=true => todas as colunas.
dbutils.widgets.text("dedup", "true", "Deduplicar? (true/false)")
dbutils.widgets.text("dedup_keys", "", "Colunas de dedup (csv; vazio=todas)")

taxi_type = dbutils.widgets.get("taxi_type")
date_start = dbutils.widgets.get("date_start")
date_stop = dbutils.widgets.get("date_stop")
raw_path = dbutils.widgets.get("raw_path")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
dedup = dbutils.widgets.get("dedup").strip().lower() == "true"
dedup_keys = [c.strip() for c in dbutils.widgets.get("dedup_keys").split(",") if c.strip()] or None

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

# DBTITLE 1,Bronze: dedup (opcional) + metadado (logica testada em tests/)
# build_bronze deduplica conforme os widgets (desligado / por chave / todas as
# colunas) e acrescenta dt_ingestion. O bronze NAO e particionado: o
# particionamento por mes explodia em diretorios orfaos por datas sujas da TLC.
print(f"Dedup: {'off' if not dedup else (dedup_keys or 'todas as colunas')}")
df_bronze = build_bronze(df_raw, dedup=dedup, dedup_keys=dedup_keys)

print(f"Colunas: {len(df_bronze.columns)} (bronze sem particao)")

# COMMAND ----------

# DBTITLE 1,Criacao do catalogo e schema (Unity Catalog)
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# DBTITLE 1,Escrita da tabela Delta (bronze)
(
    df_bronze.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(full_table)
)

print(f"Tabela {full_table} criada/atualizada (sem particao).")

# COMMAND ----------

# DBTITLE 1,Comentarios de coluna (data dictionary)
# Aplica as descricoes do data dictionary da TLC para o taxi_type em questao.
for stmt in comment_statements(full_table, comments_for(taxi_type), df_bronze.columns):
    spark.sql(stmt)
print(f"Comentarios de coluna aplicados para {taxi_type}.")

# COMMAND ----------

# DBTITLE 1,Validacao rapida
display(spark.sql(f"SELECT * FROM {full_table} LIMIT 20"))
