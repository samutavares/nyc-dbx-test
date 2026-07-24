# Databricks notebook source
# DBTITLE 1,NYC TLC - Orquestrador do Pipeline
# MAGIC %md
# MAGIC # Orquestrador do Pipeline
# MAGIC
# MAGIC Executa todas as etapas em ordem: **raw -> bronze -> silver -> analise**.
# MAGIC
# MAGIC Este notebook usa `dbutils.notebook.run(...)` para encadear os passos -
# MAGIC pratico para rodar tudo interativamente. Como alternativa declarativa,
# MAGIC use o job serverless via DAB (`databricks.yml` +
# MAGIC `resources/nyc_taxi_pipeline.job.yml`) ou o `jobs/nyc_taxi_pipeline.json`.

# COMMAND ----------

# DBTITLE 1,Parametros (widgets)
dbutils.widgets.text("taxi_type", "yellow", "Taxi type")
dbutils.widgets.text("date_start", "2023-01-01", "First month (YYYY-MM-DD)")
dbutils.widgets.text("date_stop", "2023-05-01", "Last month (YYYY-MM-DD)")
dbutils.widgets.text("catalog", "nyc_taxi", "Unity Catalog")
dbutils.widgets.text("raw_schema", "raw", "Landing schema")
dbutils.widgets.text("volume", "landing", "Landing volume")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
dbutils.widgets.text("silver_schema", "silver", "Silver schema")
dbutils.widgets.text("silver_table", "trips", "Silver table")

taxi_type = dbutils.widgets.get("taxi_type")
date_start = dbutils.widgets.get("date_start")
date_stop = dbutils.widgets.get("date_stop")
catalog = dbutils.widgets.get("catalog")
raw_schema = dbutils.widgets.get("raw_schema")
volume = dbutils.widgets.get("volume")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
silver_table = dbutils.widgets.get("silver_table")

# Caminho do Volume, derivado para as etapas seguintes.
raw_path = f"/Volumes/{catalog}/{raw_schema}/{volume}"

# Timeout por etapa (segundos). silver/bronze podem demorar em CE.
STEP_TIMEOUT = 3600

# COMMAND ----------

# DBTITLE 1,1) Raw - ingestao para a landing zone
print(">> raw_ingestion")
result = dbutils.notebook.run(
    "raw_ingestion",
    STEP_TIMEOUT,
    {
        "taxi_type": taxi_type,
        "date_start": date_start,
        "date_stop": date_stop,
        "catalog": catalog,
        "raw_schema": raw_schema,
        "volume": volume,
    },
)
print(result)

# COMMAND ----------

# DBTITLE 1,2) Bronze - replica exata (template)
print(">> bronze/template")
result = dbutils.notebook.run(
    "bronze/template",
    STEP_TIMEOUT,
    {
        "taxi_type": taxi_type,
        "date_start": date_start,
        "date_stop": date_stop,
        "raw_path": raw_path,
        "catalog": catalog,
        "schema": bronze_schema,
    },
)
print(result)

# COMMAND ----------

# DBTITLE 1,3) Silver - camada de consumo
print(">> silver_trips")
result = dbutils.notebook.run(
    "silver_trips",
    STEP_TIMEOUT,
    {
        "catalog": catalog,
        "source_schema": bronze_schema,
        "source_table": f"{taxi_type}_trips",
        "target_schema": silver_schema,
        "table": silver_table,
    },
)
print(result)

# COMMAND ----------

# DBTITLE 1,4) Analise - respostas do case
print(">> analysis/business_questions")
result = dbutils.notebook.run(
    "../analysis/business_questions",
    STEP_TIMEOUT,
    {
        "catalog": catalog,
        "schema": silver_schema,
        "table": silver_table,
    },
)
print(result)

# COMMAND ----------

# DBTITLE 1,Fim
print("Pipeline concluido: raw -> bronze -> silver -> analise")
