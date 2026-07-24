# Databricks notebook source
# DBTITLE 1,NYC TLC - Orquestrador do Pipeline
# MAGIC %md
# MAGIC # Orquestrador do Pipeline
# MAGIC
# MAGIC Executa as etapas em ordem: **raw -> bronze -> zone_lookup -> silver**,
# MAGIC para todos os tipos de taxi. A camada **gold** (analise/agregacao) sera
# MAGIC construida separadamente.
# MAGIC
# MAGIC Este notebook usa `dbutils.notebook.run(...)` para encadear os passos -
# MAGIC pratico para rodar tudo interativamente. Como alternativa declarativa,
# MAGIC use o job serverless via DAB (`databricks.yml` +
# MAGIC `resources/nyc_taxi_pipeline.job.yml`) ou o `jobs/nyc_taxi_pipeline.json`.

# COMMAND ----------

# DBTITLE 1,Parametros (widgets)
# taxi_types: lista separada por virgula. Raw + bronze + silver rodam para
# TODOS os tipos (uma tabela por tipo em cada camada).
dbutils.widgets.text("taxi_types", "yellow,green,fhv,fhvhv", "Taxi types (comma-separated)")
dbutils.widgets.text("date_start", "2023-01-01", "First month (YYYY-MM-DD)")
dbutils.widgets.text("date_stop", "2023-05-01", "Last month (YYYY-MM-DD)")
dbutils.widgets.text("catalog", "nyc_taxi", "Unity Catalog")
dbutils.widgets.text("raw_schema", "raw", "Landing schema")
dbutils.widgets.text("volume", "landing", "Landing volume")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
dbutils.widgets.text("silver_schema", "silver", "Silver schema")

taxi_types = [t.strip() for t in dbutils.widgets.get("taxi_types").split(",") if t.strip()]
date_start = dbutils.widgets.get("date_start")
date_stop = dbutils.widgets.get("date_stop")
catalog = dbutils.widgets.get("catalog")
raw_schema = dbutils.widgets.get("raw_schema")
volume = dbutils.widgets.get("volume")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")

# Caminho do Volume, derivado para as etapas seguintes.
raw_path = f"/Volumes/{catalog}/{raw_schema}/{volume}"

# Timeout por etapa (segundos). fhvhv e volumoso e pode demorar.
STEP_TIMEOUT = 3600

print(f"Tipos: {taxi_types}")

# COMMAND ----------

# DBTITLE 1,1) Raw + 2) Bronze - para cada tipo de taxi
# Ingestao (raw) e replica exata (bronze) rodam para TODOS os tipos.
for taxi_type in taxi_types:
    print(f">> raw_ingestion [{taxi_type}]")
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

    print(f">> bronze/template [{taxi_type}]")
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

# DBTITLE 1,3) Zone lookup - dimensao de zonas
print(">> zone_lookup")
result = dbutils.notebook.run(
    "zone_lookup",
    STEP_TIMEOUT,
    {
        "catalog": catalog,
        "raw_schema": raw_schema,
        "volume": volume,
        "target_schema": silver_schema,
        "table": "taxi_zone_lookup",
    },
)
print(result)

# COMMAND ----------

# DBTITLE 1,4) Silver - padronizacao leve (uma tabela por tipo)
for taxi_type in taxi_types:
    print(f">> silver/template [{taxi_type}]")
    result = dbutils.notebook.run(
        "silver/template",
        STEP_TIMEOUT,
        {
            "taxi_type": taxi_type,
            "catalog": catalog,
            "source_schema": bronze_schema,
            "target_schema": silver_schema,
            "zone_table": "taxi_zone_lookup",
        },
    )
    print(result)

# COMMAND ----------

# DBTITLE 1,Fim
print(f"Pipeline concluido para {taxi_types}: raw -> bronze -> zone_lookup -> silver (gold/analise a seguir)")
