# Databricks notebook source
# DBTITLE 1,NYC TLC - Taxi Zone Lookup (Dimensao)
# MAGIC %md
# MAGIC # Taxi Zone Lookup - Dimensao de Zonas
# MAGIC
# MAGIC Carrega o `taxi_zone_lookup.csv` da NYC TLC como uma tabela **Delta** de
# MAGIC dimensao (`nyc_taxi.silver.taxi_zone_lookup`). Ela mapeia
# MAGIC `LocationID` -> `Borough` / `Zone` / `service_zone` e e usada no silver
# MAGIC para enriquecer as corridas com origem/destino (borough e zona) e derivar
# MAGIC a flag `is_airport_trip`.
# MAGIC
# MAGIC O arquivo vem do mesmo CDN dos dados de corrida:
# MAGIC `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`.

# COMMAND ----------

# DBTITLE 1,Imports
import os
import sys

import requests
from pyspark.sql import functions as F

sys.path.insert(0, "lib")

from data_dictionary import ZONE_LOOKUP
from transforms import comment_statements, snake_case_columns

# COMMAND ----------

# DBTITLE 1,Parametros (widgets)
dbutils.widgets.text("catalog", "nyc_taxi", "Unity Catalog")
dbutils.widgets.text("raw_schema", "raw", "Landing schema")
dbutils.widgets.text("volume", "landing", "Landing volume")
dbutils.widgets.text("target_schema", "silver", "Target schema")
dbutils.widgets.text("table", "taxi_zone_lookup", "Target table")

catalog = dbutils.widgets.get("catalog")
raw_schema = dbutils.widgets.get("raw_schema")
volume = dbutils.widgets.get("volume")
target_schema = dbutils.widgets.get("target_schema")
table = dbutils.widgets.get("table")

ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
raw_path = f"/Volumes/{catalog}/{raw_schema}/{volume}"
csv_path = f"{raw_path}/taxi_zone_lookup.csv"
target_full_table = f"{catalog}.{target_schema}.{table}"

print(f"origem: {ZONE_URL}")
print(f"destino: {target_full_table}")

# COMMAND ----------

# DBTITLE 1,Criacao do catalogo, schema e volume (Unity Catalog)
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{raw_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{raw_schema}.{volume}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{target_schema}")

# COMMAND ----------

# DBTITLE 1,Download idempotente do CSV para a landing zone
os.makedirs(raw_path, exist_ok=True)
if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
    print(f"[skip] taxi_zone_lookup.csv (ja existe)")
else:
    with requests.get(ZONE_URL, stream=True, timeout=180) as res:
        res.raise_for_status()
        with open(csv_path, "wb") as file:
            for chunk in res.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file.write(chunk)
    print(f"[ok] taxi_zone_lookup.csv ({os.path.getsize(csv_path) / 1024:.1f} KB)")

# COMMAND ----------

# DBTITLE 1,Leitura + snake_case + tipagem
# snake_case em todas as colunas (LocationID->location_id, Borough->borough, ...)
df_zone = snake_case_columns(spark.read.option("header", "true").csv(csv_path))
df_zone = df_zone.withColumn("location_id", F.col("location_id").cast("int"))

display(df_zone)

# COMMAND ----------

# DBTITLE 1,Escrita da dimensao Delta
(
    df_zone.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_full_table)
)
print(f"Tabela {target_full_table} criada/atualizada.")

# COMMAND ----------

# DBTITLE 1,Comentarios de coluna (data dictionary)
for stmt in comment_statements(target_full_table, ZONE_LOOKUP, df_zone.columns):
    spark.sql(stmt)
print("Comentarios aplicados na dimensao de zonas.")
