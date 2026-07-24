# Databricks notebook source
# DBTITLE 1,NYC TLC - Silver (Consumption Layer)
# MAGIC %md
# MAGIC # Silver - Camada de Consumo (PySpark + Delta)
# MAGIC
# MAGIC Le a **replica exata** da camada bronze, aplica limpeza/tipagem com
# MAGIC **PySpark** e materializa uma tabela **Delta** consultavel via SQL.
# MAGIC
# MAGIC Garante as colunas obrigatorias exigidas pelo case:
# MAGIC `VendorID`, `passenger_count`, `total_amount`,
# MAGIC `tpep_pickup_datetime`, `tpep_dropoff_datetime`.

# COMMAND ----------

# DBTITLE 1,Imports
import sys

sys.path.insert(0, "lib")

from transforms import REQUIRED_COLUMNS, build_silver

# COMMAND ----------

# DBTITLE 1,Parametros (widgets)
dbutils.widgets.text("catalog", "nyc_taxi", "Unity Catalog")
dbutils.widgets.text("source_schema", "bronze", "Source bronze schema")
dbutils.widgets.text("source_table", "yellow_trips", "Source bronze table")
dbutils.widgets.text("target_schema", "silver", "Target silver schema")
dbutils.widgets.text("table", "trips", "Target table")

catalog = dbutils.widgets.get("catalog")
source_schema = dbutils.widgets.get("source_schema")
source_table = dbutils.widgets.get("source_table")
target_schema = dbutils.widgets.get("target_schema")
table = dbutils.widgets.get("table")

source_full_table = f"{catalog}.{source_schema}.{source_table}"
target_full_table = f"{catalog}.{target_schema}.{table}"

print(f"Lendo: {source_full_table}")
print(f"Gravando: {target_full_table}")

# COMMAND ----------

# DBTITLE 1,Leitura da camada bronze
df_raw = spark.read.table(source_full_table)

print(f"Linhas lidas: {df_raw.count():,}")
missing = [c for c in REQUIRED_COLUMNS if c not in df_raw.columns]
assert not missing, f"Colunas obrigatorias ausentes na origem: {missing}"

# COMMAND ----------

# DBTITLE 1,Selecao + tipagem + limpeza (logica testada em tests/)
df_silver = build_silver(df_raw)

print(f"Linhas apos limpeza: {df_silver.count():,}")

# COMMAND ----------

# DBTITLE 1,Criacao do catalogo e schema (Unity Catalog)
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{target_schema}")

# COMMAND ----------

# DBTITLE 1,Escrita da tabela Delta (consumo via SQL)
(
    df_silver.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("pickup_year", "pickup_month")
    .saveAsTable(target_full_table)
)

print(f"Tabela {target_full_table} criada/atualizada.")

# COMMAND ----------

# DBTITLE 1,Validacao rapida
display(
    spark.sql(
        f"""
        SELECT pickup_year, pickup_month, COUNT(*) AS trips
        FROM {target_full_table}
        GROUP BY pickup_year, pickup_month
        ORDER BY pickup_year, pickup_month
        """
    )
)
