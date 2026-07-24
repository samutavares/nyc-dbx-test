-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold - Agregacoes e cubo analitico
-- MAGIC
-- MAGIC Materializa, sobre o star schema, as **tabelas agregadas** pedidas pelo
-- MAGIC case e um **cubo analitico** (`GROUP BY CUBE`) com as combinacoes de
-- MAGIC dimensoes mais usadas. Depende de `fact_trips.sql` e `dimensions.sql`.
-- MAGIC
-- MAGIC Perguntas do case (respondidas nas queries ao final deste notebook):
-- MAGIC 2. Media de `total_amount` recebido em um mes (yellow) -> `agg_revenue_monthly`.
-- MAGIC 3. Media de `passenger_count` por hora do dia (Maio, todos os tipos) -> fato + `dim_time`.

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

-- MAGIC %md
-- MAGIC ## Respostas do case (queries gold)

-- COMMAND ----------

-- DBTITLE 1,Pergunta 2 - media de total_amount recebido em um mes (yellow)
-- MAGIC %md
-- MAGIC **Q2.** Qual a media de valor total (`total_amount`) recebido em um mes
-- MAGIC considerando todos os yellow taxis da frota?
-- MAGIC
-- MAGIC A media por corrida ja esta materializada em `agg_revenue_monthly`
-- MAGIC (`avg_total_amount`). Trazemos tambem o total do mes (`sum_total_amount`) e,
-- MAGIC na ultima linha, a media dos totais mensais (media de "quanto entra por mes").

-- COMMAND ----------

-- Media de total_amount POR CORRIDA, mes a mes (e o total recebido no mes).
SELECT
  year,
  month,
  trips,
  ROUND(avg_total_amount, 2) AS avg_total_amount_por_corrida,
  ROUND(sum_total_amount, 2) AS total_recebido_no_mes
FROM nyc_taxi.gold.agg_revenue_monthly
WHERE service_type = 'yellow'
ORDER BY year, month;

-- COMMAND ----------

-- Media do valor total recebido POR MES (media dos totais mensais) - yellow.
SELECT
  ROUND(AVG(sum_total_amount), 2) AS media_total_recebido_por_mes,
  ROUND(AVG(avg_total_amount), 2) AS media_total_amount_por_corrida
FROM nyc_taxi.gold.agg_revenue_monthly
WHERE service_type = 'yellow';

-- COMMAND ----------

-- DBTITLE 1,Pergunta 3 - media de passenger_count por hora do dia (Maio, todos os tipos)
-- MAGIC %md
-- MAGIC **Q3.** Qual a media de passageiros (`passenger_count`) por cada hora do dia
-- MAGIC que pegaram taxi no mes de maio considerando todos os taxis da frota?
-- MAGIC
-- MAGIC Consulta direta ao fato para media ponderada correta (nao media de medias).
-- MAGIC `passenger_count` e NULL para FHV/FHVHV, entao `AVG` ja ignora esses registros
-- MAGIC e o resultado reflete os tipos que reportam passageiros (yellow/green).

-- COMMAND ----------

SELECT
  t.hour,
  t.period_of_day,
  ROUND(AVG(f.passenger_count), 3) AS avg_passenger_count,
  COUNT(f.passenger_count)         AS trips_com_passageiros
FROM nyc_taxi.gold.fact_trips f
JOIN nyc_taxi.gold.dim_date d ON f.pickup_date_key = d.date_key
JOIN nyc_taxi.gold.dim_time t ON f.pickup_time_key = t.time_key
WHERE d.month = 5
GROUP BY t.hour, t.period_of_day
ORDER BY t.hour;
