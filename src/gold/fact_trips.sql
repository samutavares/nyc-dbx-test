-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold - Fato `fact_trips` (star schema)
-- MAGIC
-- MAGIC Unifica as quatro tabelas silver (`yellow/green/fhv/fhvhv`) em um unico
-- MAGIC fato de grao **uma corrida**, com medidas conformadas e as chaves
-- MAGIC substitutas (FK) das dimensoes. Medidas inexistentes num tipo ficam
-- MAGIC `NULL`; chaves sem correspondencia caem no membro desconhecido (0/-1).
-- MAGIC Depende de `dimensions.sql` (dimensoes ja criadas).

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS nyc_taxi.gold;

-- COMMAND ----------

-- DBTITLE 1,fact_trips (UNION dos tipos + join com dimensoes)
CREATE OR REPLACE TABLE nyc_taxi.gold.fact_trips
PARTITIONED BY (service_type_key)
COMMENT 'Fato de corridas (grao = 1 corrida) unificando os quatro tipos de taxi.'
AS
WITH unified AS (
  -- Yellow
  SELECT
    'yellow'                                   AS service_type,
    CAST(vendor_id AS BIGINT)                  AS vendor_id,
    CAST(ratecode_id AS DOUBLE)                AS ratecode_id,
    CAST(payment_type AS DOUBLE)               AS payment_type,
    CAST(NULL AS STRING)                       AS hvfhs_license_num,
    tpep_pickup_datetime                       AS pickup_datetime,
    tpep_dropoff_datetime                      AS dropoff_datetime,
    CAST(pu_location_id AS INT)                AS pu_location_id,
    CAST(do_location_id AS INT)                AS do_location_id,
    CAST(passenger_count AS INT)               AS passenger_count,
    CAST(trip_distance AS DOUBLE)              AS trip_distance,
    CAST(fare_amount AS DOUBLE)                AS fare_amount,
    CAST(tip_amount AS DOUBLE)                 AS tip_amount,
    CAST(tolls_amount AS DOUBLE)               AS tolls_amount,
    CAST(total_amount AS DOUBLE)               AS total_amount,
    is_airport_trip
  FROM nyc_taxi.silver.yellow_trips

  UNION ALL

  -- Green
  SELECT
    'green',
    CAST(vendor_id AS BIGINT),
    CAST(ratecode_id AS DOUBLE),
    CAST(payment_type AS DOUBLE),
    CAST(NULL AS STRING),
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    CAST(pu_location_id AS INT),
    CAST(do_location_id AS INT),
    CAST(passenger_count AS INT),
    CAST(trip_distance AS DOUBLE),
    CAST(fare_amount AS DOUBLE),
    CAST(tip_amount AS DOUBLE),
    CAST(tolls_amount AS DOUBLE),
    CAST(total_amount AS DOUBLE),
    is_airport_trip
  FROM nyc_taxi.silver.green_trips

  UNION ALL

  -- FHV (sem tarifa/passageiros)
  SELECT
    'fhv',
    CAST(NULL AS BIGINT),
    CAST(NULL AS DOUBLE),
    CAST(NULL AS DOUBLE),
    CAST(NULL AS STRING),
    pickup_datetime,
    drop_off_datetime,
    CAST(pu_location_id AS INT),
    CAST(do_location_id AS INT),
    CAST(NULL AS INT),
    CAST(NULL AS DOUBLE),
    CAST(NULL AS DOUBLE),
    CAST(NULL AS DOUBLE),
    CAST(NULL AS DOUBLE),
    CAST(NULL AS DOUBLE),
    is_airport_trip
  FROM nyc_taxi.silver.fhv_trips

  UNION ALL

  -- FHVHV (tarifa = componentes; total = soma)
  SELECT
    'fhvhv',
    CAST(NULL AS BIGINT),
    CAST(NULL AS DOUBLE),
    CAST(NULL AS DOUBLE),
    hvfhs_license_num,
    pickup_datetime,
    dropoff_datetime,
    CAST(pu_location_id AS INT),
    CAST(do_location_id AS INT),
    CAST(NULL AS INT),
    CAST(trip_miles AS DOUBLE),
    CAST(base_passenger_fare AS DOUBLE),
    CAST(tips AS DOUBLE),
    CAST(tolls AS DOUBLE),
    CAST(
      COALESCE(base_passenger_fare, 0) + COALESCE(tolls, 0) + COALESCE(bcf, 0)
      + COALESCE(sales_tax, 0) + COALESCE(congestion_surcharge, 0)
      + COALESCE(airport_fee, 0) + COALESCE(tips, 0) AS DOUBLE
    ),
    is_airport_trip
  FROM nyc_taxi.silver.fhvhv_trips
)
SELECT
  monotonically_increasing_id()                              AS trip_sk,
  CAST(date_format(u.pickup_datetime,  'yyyyMMdd') AS INT)   AS pickup_date_key,
  HOUR(u.pickup_datetime)                                    AS pickup_time_key,
  CAST(date_format(u.dropoff_datetime, 'yyyyMMdd') AS INT)   AS dropoff_date_key,
  COALESCE(puz.zone_key, -1)                                 AS pickup_zone_key,
  COALESCE(doz.zone_key, -1)                                 AS dropoff_zone_key,
  COALESCE(v.vendor_key, 0)                                  AS vendor_key,
  COALESCE(r.rate_code_key, 0)                               AS rate_code_key,
  COALESCE(p.payment_type_key, 0)                            AS payment_type_key,
  COALESCE(l.hvfhs_license_key, 0)                           AS hvfhs_license_key,
  st.service_type_key,
  u.pickup_datetime,
  u.dropoff_datetime,
  u.trip_distance,
  CAST((unix_timestamp(u.dropoff_datetime) - unix_timestamp(u.pickup_datetime)) / 60 AS INT) AS trip_duration_min,
  u.passenger_count,
  u.fare_amount,
  u.tip_amount,
  u.tolls_amount,
  u.total_amount,
  u.is_airport_trip
FROM unified u
LEFT JOIN nyc_taxi.gold.dim_zone         puz ON u.pu_location_id     = puz.zone_key
LEFT JOIN nyc_taxi.gold.dim_zone         doz ON u.do_location_id     = doz.zone_key
LEFT JOIN nyc_taxi.gold.dim_vendor       v   ON u.vendor_id          = v.vendor_key
LEFT JOIN nyc_taxi.gold.dim_rate_code    r   ON CAST(u.ratecode_id AS INT)   = r.rate_code_key
LEFT JOIN nyc_taxi.gold.dim_payment_type p   ON CAST(u.payment_type AS INT)  = p.payment_type_key
LEFT JOIN nyc_taxi.gold.dim_hvfhs_license l  ON u.hvfhs_license_num  = l.hvfhs_license_num
JOIN      nyc_taxi.gold.dim_service_type st  ON u.service_type       = st.service_type;

-- COMMAND ----------

-- DBTITLE 1,Validacao (contagem por tipo)
SELECT st.service_type, COUNT(*) AS trips
FROM nyc_taxi.gold.fact_trips f
JOIN nyc_taxi.gold.dim_service_type st ON f.service_type_key = st.service_type_key
GROUP BY st.service_type
ORDER BY trips DESC;
