-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold - Agregacoes e cubo analitico
-- MAGIC
-- MAGIC Materializa, sobre o star schema, as **tabelas agregadas** pedidas pelo
-- MAGIC case e um **cubo analitico** (`GROUP BY CUBE`) com as combinacoes de
-- MAGIC dimensoes mais usadas. Depende de `fact_trips.sql` e `dimensions.sql`.
-- MAGIC
-- MAGIC Perguntas do case (respondidas filtrando as tabelas abaixo):
-- MAGIC 1. Media de `total_amount` por mes -> `agg_revenue_monthly`.
-- MAGIC 2. Media de `passenger_count` por hora do dia (Maio) -> `agg_trips_by_hour`.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS nyc_taxi.gold;

-- COMMAND ----------

-- DBTITLE 1,agg_revenue_monthly (receita/mes por tipo)
CREATE OR REPLACE TABLE nyc_taxi.gold.agg_revenue_monthly
COMMENT 'Metricas mensais por tipo de servico. Responde a media de total_amount por mes.' AS
SELECT
  st.service_type,
  d.year,
  d.month,
  COUNT(*)                    AS trips,
  AVG(f.total_amount)         AS avg_total_amount,
  SUM(f.total_amount)         AS sum_total_amount,
  AVG(f.trip_distance)        AS avg_trip_distance,
  AVG(f.trip_duration_min)    AS avg_trip_duration_min,
  AVG(f.tip_amount)           AS avg_tip_amount
FROM nyc_taxi.gold.fact_trips f
JOIN nyc_taxi.gold.dim_service_type st ON f.service_type_key = st.service_type_key
JOIN nyc_taxi.gold.dim_date d          ON f.pickup_date_key  = d.date_key
GROUP BY st.service_type, d.year, d.month;

-- COMMAND ----------

-- DBTITLE 1,agg_trips_by_hour (corridas por hora do dia)
CREATE OR REPLACE TABLE nyc_taxi.gold.agg_trips_by_hour
COMMENT 'Metricas por hora do dia e mes por tipo. Responde a media de passenger_count por hora.' AS
SELECT
  st.service_type,
  d.year,
  d.month,
  t.hour,
  t.period_of_day,
  COUNT(*)                 AS trips,
  AVG(f.passenger_count)   AS avg_passenger_count,
  AVG(f.total_amount)      AS avg_total_amount
FROM nyc_taxi.gold.fact_trips f
JOIN nyc_taxi.gold.dim_service_type st ON f.service_type_key = st.service_type_key
JOIN nyc_taxi.gold.dim_date d          ON f.pickup_date_key  = d.date_key
JOIN nyc_taxi.gold.dim_time t          ON f.pickup_time_key  = t.time_key
GROUP BY st.service_type, d.year, d.month, t.hour, t.period_of_day;

-- COMMAND ----------

-- DBTITLE 1,agg_trips_by_zone (volume/receita por borough e zona de embarque)
CREATE OR REPLACE TABLE nyc_taxi.gold.agg_trips_by_zone
COMMENT 'Metricas por borough/zona de embarque e tipo de servico.' AS
SELECT
  st.service_type,
  puz.borough      AS pickup_borough,
  puz.zone         AS pickup_zone,
  COUNT(*)              AS trips,
  AVG(f.total_amount)   AS avg_total_amount,
  AVG(f.trip_distance)  AS avg_trip_distance,
  SUM(CAST(f.is_airport_trip AS INT)) AS airport_trips
FROM nyc_taxi.gold.fact_trips f
JOIN nyc_taxi.gold.dim_service_type st ON f.service_type_key = st.service_type_key
JOIN nyc_taxi.gold.dim_zone puz        ON f.pickup_zone_key  = puz.zone_key
GROUP BY st.service_type, puz.borough, puz.zone;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cubo analitico (`cube_trips`)
-- MAGIC
-- MAGIC Um unico `GROUP BY CUBE` sobre as 6 dimensoes mais usadas
-- MAGIC (`service_type`, `year`, `month`, `hour`, `pickup_borough`,
-- MAGIC `payment_type_name`) gera todos os subtotais (2^6 grouping sets). A coluna
-- MAGIC `grouping_id` identifica o nivel de agregacao: `NULL` numa dimensao =
-- MAGIC "todos" (rollup); use `grouping_id = 0` para o grao mais detalhado.

-- COMMAND ----------

-- DBTITLE 1,cube_trips (GROUP BY CUBE)
CREATE OR REPLACE TABLE nyc_taxi.gold.cube_trips
COMMENT 'Cubo OLAP: subtotais por service_type/year/month/hour/pickup_borough/payment_type via GROUP BY CUBE.' AS
SELECT
  st.service_type,
  d.year,
  d.month,
  t.hour,
  puz.borough           AS pickup_borough,
  pt.payment_type_name,
  GROUPING_ID(st.service_type, d.year, d.month, t.hour, puz.borough, pt.payment_type_name) AS grouping_id,
  COUNT(*)                  AS trips,
  AVG(f.total_amount)       AS avg_total_amount,
  SUM(f.total_amount)       AS sum_total_amount,
  AVG(f.passenger_count)    AS avg_passenger_count,
  AVG(f.trip_distance)      AS avg_trip_distance,
  AVG(f.trip_duration_min)  AS avg_trip_duration_min,
  AVG(f.tip_amount)         AS avg_tip_amount
FROM nyc_taxi.gold.fact_trips f
JOIN      nyc_taxi.gold.dim_service_type st ON f.service_type_key = st.service_type_key
JOIN      nyc_taxi.gold.dim_date d          ON f.pickup_date_key  = d.date_key
JOIN      nyc_taxi.gold.dim_time t          ON f.pickup_time_key  = t.time_key
LEFT JOIN nyc_taxi.gold.dim_zone puz        ON f.pickup_zone_key  = puz.zone_key
LEFT JOIN nyc_taxi.gold.dim_payment_type pt ON f.payment_type_key = pt.payment_type_key
GROUP BY CUBE (st.service_type, d.year, d.month, t.hour, puz.borough, pt.payment_type_name);

-- COMMAND ----------

-- DBTITLE 1,Case Q1 - media de total_amount por mes (yellow)
SELECT year, month, avg_total_amount, trips
FROM nyc_taxi.gold.agg_revenue_monthly
WHERE service_type = 'yellow'
ORDER BY year, month;

-- COMMAND ----------

-- DBTITLE 1,Case Q2 - media de passenger_count por hora (Maio, todos os tipos)
-- Consulta direta ao fato para media ponderada correta (nao media de medias).
SELECT
  t.hour,
  ROUND(AVG(f.passenger_count), 3) AS avg_passenger_count,
  COUNT(f.passenger_count)         AS trips_com_passageiros
FROM nyc_taxi.gold.fact_trips f
JOIN nyc_taxi.gold.dim_date d ON f.pickup_date_key = d.date_key
JOIN nyc_taxi.gold.dim_time t ON f.pickup_time_key = t.time_key
WHERE d.month = 5
GROUP BY t.hour
ORDER BY t.hour;
