# Databricks notebook source
# DBTITLE 1,NYC TLC - Analises (Respostas do Case)
# MAGIC %md
# MAGIC # Analises - Respostas das Perguntas do Case
# MAGIC
# MAGIC Consultas sobre a tabela de consumo `nyc_taxi.silver.trips`.
# MAGIC
# MAGIC 1. Media de `total_amount` recebido por mes (todos os yellow taxis).
# MAGIC 2. Media de `passenger_count` por hora do dia em **Maio** (todos os taxis).

# COMMAND ----------

# DBTITLE 1,Parametros (widgets)
dbutils.widgets.text("catalog", "nyc_taxi", "Unity Catalog")
dbutils.widgets.text("schema", "silver", "Schema")
dbutils.widgets.text("table", "trips", "Table")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table = dbutils.widgets.get("table")
database_table = f"{catalog}.{schema}.{table}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 1
# MAGIC **Qual a media de valor total (`total_amount`) recebido em um mes,
# MAGIC considerando todos os yellow taxis da frota?**
# MAGIC
# MAGIC Interpretacao: media do `total_amount` por corrida, agregada por mes.

# COMMAND ----------

# DBTITLE 1,Q1 - Media de total_amount por mes
df_q1 = spark.sql(
    f"""
    SELECT
        pickup_year                      AS ano,
        pickup_month                     AS mes,
        COUNT(*)                         AS qt_corridas,
        ROUND(AVG(total_amount), 2)      AS media_total_amount,
        ROUND(SUM(total_amount), 2)      AS soma_total_amount
    FROM {database_table}
    GROUP BY pickup_year, pickup_month
    ORDER BY pickup_year, pickup_month
    """
)
display(df_q1)

# COMMAND ----------

# DBTITLE 1,Q1 - Media geral do periodo (visao unica)
df_q1_geral = spark.sql(
    f"""
    SELECT ROUND(AVG(total_amount), 2) AS media_total_amount_periodo
    FROM {database_table}
    """
)
display(df_q1_geral)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 2
# MAGIC **Qual a media de passageiros (`passenger_count`) por cada hora do dia
# MAGIC que pegaram taxi no mes de Maio, considerando todos os taxis da frota?**

# COMMAND ----------

# DBTITLE 1,Q2 - Media de passageiros por hora do dia (Maio)
df_q2 = spark.sql(
    f"""
    SELECT
        HOUR(tpep_pickup_datetime)          AS hora_do_dia,
        COUNT(*)                            AS qt_corridas,
        ROUND(AVG(passenger_count), 2)      AS media_passageiros
    FROM {database_table}
    WHERE pickup_month = 5
    GROUP BY HOUR(tpep_pickup_datetime)
    ORDER BY hora_do_dia
    """
)
display(df_q2)

# COMMAND ----------

# DBTITLE 1,Q2 - Grafico (media de passageiros por hora)
# No Databricks, use o botao de grafico do display() acima (barras: x=hora_do_dia, y=media_passageiros).
# Alternativa programatica com matplotlib:
pdf = df_q2.toPandas()
import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize=(10, 4))
ax.bar(pdf["hora_do_dia"], pdf["media_passageiros"])
ax.set_xlabel("Hora do dia")
ax.set_ylabel("Media de passageiros")
ax.set_title("Media de passageiros por hora do dia - Maio/2023")
ax.set_xticks(range(0, 24))
plt.tight_layout()
display(fig)
