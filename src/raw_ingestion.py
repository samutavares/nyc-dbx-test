# Databricks notebook source
# DBTITLE 1,NYC TLC - Raw Ingestion (Landing Zone)
# MAGIC %md
# MAGIC # Raw Ingestion - NYC TLC Trip Data
# MAGIC
# MAGIC Baixa os arquivos Parquet mensais originais do site da NYC TLC e os
# MAGIC armazena, **sem transformacao**, em uma landing zone num **Unity Catalog
# MAGIC Volume** (`/Volumes/<catalog>/<schema>/<volume>`).
# MAGIC
# MAGIC A NYC TLC publica arquivos Parquet mensais estaticos em um CDN
# MAGIC (CloudFront) com um padrao de URL previsivel:
# MAGIC
# MAGIC ```
# MAGIC https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{YYYY}-{MM}.parquet
# MAGIC ```
# MAGIC
# MAGIC Portanto NAO ha necessidade de scraping de HTML nem de autenticacao:
# MAGIC parametrizamos `taxi_type` + intervalo de datas, montamos a URL e
# MAGIC fazemos o streaming do binario direto para o Volume. Volumes sao o
# MAGIC armazenamento governado recomendado na Databricks Free Edition (o DBFS
# MAGIC root e legado).

# COMMAND ----------

# DBTITLE 1,Imports
import os
import sys

import requests

sys.path.insert(0, "lib")

from transforms import month_list

# COMMAND ----------

# DBTITLE 1,Parametros (widgets)
# Rode esta celula uma vez para criar os widgets; depois ajuste os valores
# no topo do notebook (ou via dbutils.notebook.run).
dbutils.widgets.text("taxi_type", "yellow", "Taxi type (yellow/green/fhv/fhvhv)")
dbutils.widgets.text("date_start", "2023-01-01", "First month (YYYY-MM-DD)")
dbutils.widgets.text("date_stop", "2023-05-01", "Last month (YYYY-MM-DD)")
dbutils.widgets.text("catalog", "nyc_taxi", "Unity Catalog")
dbutils.widgets.text("raw_schema", "raw", "Landing schema")
dbutils.widgets.text("volume", "landing", "Landing volume")

taxi_type = dbutils.widgets.get("taxi_type")
date_start = dbutils.widgets.get("date_start")
date_stop = dbutils.widgets.get("date_stop")
catalog = dbutils.widgets.get("catalog")
raw_schema = dbutils.widgets.get("raw_schema")
volume = dbutils.widgets.get("volume")

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Caminho do Volume (acessivel tanto pela API POSIX quanto pelo Spark).
raw_path = f"/Volumes/{catalog}/{raw_schema}/{volume}"

print(f"taxi_type={taxi_type} | {date_start} -> {date_stop}")
print(f"landing zone (volume): {raw_path}")

# COMMAND ----------

# DBTITLE 1,Criacao do catalogo, schema e volume (Unity Catalog)
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{raw_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{raw_schema}.{volume}")

# COMMAND ----------

# DBTITLE 1,Helpers


def download_month(taxi_type: str, year_month: str, dest_dir: str) -> bool:
    """Faz streaming de um Parquet mensal para a landing zone. Idempotente."""
    filename = f"{taxi_type}_tripdata_{year_month}.parquet"
    url = f"{BASE_URL}/{filename}"
    dest = os.path.join(dest_dir, filename)

    os.makedirs(dest_dir, exist_ok=True)

    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        print(f"[skip] {filename} (ja existe)")
        return True

    with requests.get(url, stream=True, timeout=180) as res:
        if res.status_code != 200:
            print(f"[falha] {filename} -> HTTP {res.status_code}")
            return False
        with open(dest, "wb") as file:
            for chunk in res.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file.write(chunk)

    size_mb = os.path.getsize(dest) / (1024 * 1024)
    print(f"[ok] {filename} ({size_mb:.1f} MB)")
    return True

# COMMAND ----------

# DBTITLE 1,Execucao
months = month_list(date_start, date_stop)
print(f"Meses a ingerir: {months}")

results = {ym: download_month(taxi_type, ym, raw_path) for ym in months}

# COMMAND ----------

# DBTITLE 1,Conferencia dos arquivos na landing zone
display(dbutils.fs.ls(raw_path))

# COMMAND ----------

# DBTITLE 1,Resumo
ok = [ym for ym, success in results.items() if success]
fail = [ym for ym, success in results.items() if not success]
print(f"Ingeridos: {ok}")
if fail:
    print(f"Falharam (verifique se o mes ja foi publicado pela TLC): {fail}")
