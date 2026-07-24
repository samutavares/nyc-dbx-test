# Databricks notebook source
# DBTITLE 1,NYC TLC - Silver (Standardized Template)
# MAGIC %md
# MAGIC # Silver - Padronizacao Leve (Template por tipo de taxi)
# MAGIC
# MAGIC Le a tabela bronze de um `taxi_type` e materializa uma tabela **Delta**
# MAGIC silver `nyc_taxi.silver.<taxi_type>_trips`, aplicando apenas
# MAGIC **transformacoes leves** e **mantendo TODAS as colunas**:
# MAGIC
# MAGIC - converte todos os nomes de coluna para **snake_case**;
# MAGIC - tipa as colunas de data/hora (timestamp) e de zona (int);
# MAGIC - deriva `pickup_year`/`pickup_month` (particao);
# MAGIC - enriquece com `taxi_zone_lookup` (borough/zona + `is_airport_trip`).
# MAGIC
# MAGIC Nenhuma linha e filtrada e nenhuma coluna e descartada - selecoes,
# MAGIC limpezas e agregacoes de negocio ficam para a camada **gold**.

# COMMAND ----------

# DBTITLE 1,Imports
import sys

sys.path.insert(0, "../lib")

from data_dictionary import silver_comments_for
from transforms import comment_statements, standardize_silver

# COMMAND ----------

# DBTITLE 1,Parametros (widgets)
dbutils.widgets.text("taxi_type", "yellow", "Taxi type (yellow/green/fhv/fhvhv)")
dbutils.widgets.text("catalog", "nyc_taxi", "Unity Catalog")
dbutils.widgets.text("source_schema", "bronze", "Source bronze schema")
dbutils.widgets.text("target_schema", "silver", "Target silver schema")
dbutils.widgets.text("zone_table", "taxi_zone_lookup", "Zone lookup dimension table")

taxi_type = dbutils.widgets.get("taxi_type")
catalog = dbutils.widgets.get("catalog")
source_schema = dbutils.widgets.get("source_schema")
target_schema = dbutils.widgets.get("target_schema")
zone_table = dbutils.widgets.get("zone_table")

table = f"{taxi_type}_trips"
source_full_table = f"{catalog}.{source_schema}.{table}"
target_full_table = f"{catalog}.{target_schema}.{table}"
zone_full_table = f"{catalog}.{target_schema}.{zone_table}"

print(f"taxi_type={taxi_type}")
print(f"Lendo: {source_full_table}")
print(f"Zonas: {zone_full_table}")
print(f"Gravando: {target_full_table}")

# COMMAND ----------

# DBTITLE 1,Leitura da camada bronze
df_raw = spark.read.table(source_full_table)
print(f"Linhas lidas: {df_raw.count():,}")

# COMMAND ----------

# DBTITLE 1,Dimensao de zonas (para enriquecimento)
if spark.catalog.tableExists(zone_full_table):
    df_zone = spark.read.table(zone_full_table)
    print(f"Zonas carregadas: {df_zone.count()}")
else:
    df_zone = None
    print(f"[aviso] {zone_full_table} nao encontrada; silver sem enriquecimento de zonas.")

# COMMAND ----------

# DBTITLE 1,Padronizacao leve (snake_case + tipagem + particao + zonas)
df_silver = standardize_silver(df_raw, zone_df=df_zone)

print(f"Linhas (mantidas todas): {df_silver.count():,}")
print(f"Colunas: {df_silver.columns}")

# COMMAND ----------

# DBTITLE 1,Criacao do catalogo e schema (Unity Catalog)
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{target_schema}")

# COMMAND ----------

# DBTITLE 1,Escrita da tabela Delta silver
writer = (
    df_silver.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
)

partition_by = [c for c in ("pickup_year", "pickup_month") if c in df_silver.columns]
if partition_by:
    writer = writer.partitionBy(*partition_by)

writer.saveAsTable(target_full_table)
print(f"Tabela {target_full_table} criada/atualizada. Particao: {partition_by}")

# COMMAND ----------

# DBTITLE 1,Comentarios de coluna (data dictionary)
for stmt in comment_statements(target_full_table, silver_comments_for(taxi_type), df_silver.columns):
    spark.sql(stmt)
print(f"Comentarios de coluna aplicados para {taxi_type}.")

# COMMAND ----------

# DBTITLE 1,Validacao rapida
display(spark.sql(f"SELECT * FROM {target_full_table} LIMIT 20"))
