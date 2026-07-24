-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold - Dimensoes (star schema)
-- MAGIC
-- MAGIC Cria as **dimensoes conformadas** do esquema estrela em `nyc_taxi.gold`.
-- MAGIC As dimensoes de codigo (vendor/ratecode/payment/hvfhs) sao estaticas e
-- MAGIC incluem um membro "desconhecido" (chave `0`/`-1`) para linhas sem match.
-- MAGIC `dim_zone` vem de `nyc_taxi.silver.taxi_zone_lookup`; `dim_date`/`dim_time`
-- MAGIC sao geradas. Catalogo fixo: `nyc_taxi`.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS nyc_taxi;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS nyc_taxi.gold;

-- COMMAND ----------

-- DBTITLE 1,dim_service_type
CREATE OR REPLACE TABLE nyc_taxi.gold.dim_service_type
COMMENT 'Tipo de servico de taxi (yellow/green/fhv/fhvhv).' AS
SELECT * FROM VALUES
  (1, 'yellow', 'Yellow Taxi'),
  (2, 'green',  'Green Taxi'),
  (3, 'fhv',    'For-Hire Vehicle'),
  (4, 'fhvhv',  'High-Volume For-Hire Vehicle')
AS t(service_type_key, service_type, description);

-- COMMAND ----------

-- DBTITLE 1,dim_vendor
CREATE OR REPLACE TABLE nyc_taxi.gold.dim_vendor
COMMENT 'Provedor do registro (TPEP/LPEP). Chave 0 = desconhecido.' AS
SELECT * FROM VALUES
  (0, 'Desconhecido'),
  (1, 'Creative Mobile Technologies'),
  (2, 'VeriFone Inc.')
AS t(vendor_key, vendor_name);

-- COMMAND ----------

-- DBTITLE 1,dim_rate_code
CREATE OR REPLACE TABLE nyc_taxi.gold.dim_rate_code
COMMENT 'Codigo de tarifa vigente no fim da corrida. Chave 0 = desconhecido.' AS
SELECT * FROM VALUES
  (0, 'Desconhecido'),
  (1, 'Standard'),
  (2, 'JFK'),
  (3, 'Newark'),
  (4, 'Nassau/Westchester'),
  (5, 'Negociada'),
  (6, 'Group ride')
AS t(rate_code_key, rate_code_name);

-- COMMAND ----------

-- DBTITLE 1,dim_payment_type
CREATE OR REPLACE TABLE nyc_taxi.gold.dim_payment_type
COMMENT 'Forma de pagamento. Chave 0 = nao informado.' AS
SELECT * FROM VALUES
  (0, 'Nao informado'),
  (1, 'Cartao de credito'),
  (2, 'Dinheiro'),
  (3, 'Sem cobranca'),
  (4, 'Disputa'),
  (5, 'Desconhecido'),
  (6, 'Corrida anulada')
AS t(payment_type_key, payment_type_name);

-- COMMAND ----------

-- DBTITLE 1,dim_hvfhs_license
CREATE OR REPLACE TABLE nyc_taxi.gold.dim_hvfhs_license
COMMENT 'Empresa HVFHS (fhvhv). Chave 0 = nao aplicavel.' AS
SELECT * FROM VALUES
  (0, 'N/A',    'Nao aplicavel'),
  (1, 'HV0002', 'Juno'),
  (2, 'HV0003', 'Uber'),
  (3, 'HV0004', 'Via'),
  (4, 'HV0005', 'Lyft')
AS t(hvfhs_license_key, hvfhs_license_num, hvfhs_license_name);

-- COMMAND ----------

-- DBTITLE 1,dim_zone (a partir do silver + membro desconhecido)
CREATE OR REPLACE TABLE nyc_taxi.gold.dim_zone
COMMENT 'Dimensao de zonas TLC (role-playing: embarque/desembarque). Chave -1 = desconhecida.' AS
SELECT -1 AS zone_key, -1 AS location_id, 'Unknown' AS borough, 'Unknown' AS zone, 'Unknown' AS service_zone
UNION ALL
SELECT location_id AS zone_key, location_id, borough, zone, service_zone
FROM nyc_taxi.silver.taxi_zone_lookup;

-- COMMAND ----------

-- DBTITLE 1,dim_time (0-23h + periodo do dia)
CREATE OR REPLACE TABLE nyc_taxi.gold.dim_time
COMMENT 'Dimensao de hora do dia (0-23) com periodo do dia.' AS
SELECT
  h AS time_key,
  h AS hour,
  CASE
    WHEN h BETWEEN 0 AND 5  THEN 'Madrugada'
    WHEN h BETWEEN 6 AND 11 THEN 'Manha'
    WHEN h BETWEEN 12 AND 17 THEN 'Tarde'
    ELSE 'Noite'
  END AS period_of_day
FROM (SELECT explode(sequence(0, 23)) AS h);

-- COMMAND ----------

-- DBTITLE 1,dim_date (calendario derivado do intervalo do silver)
CREATE OR REPLACE TABLE nyc_taxi.gold.dim_date
COMMENT 'Dimensao de calendario (grao = dia) cobrindo o periodo carregado no silver.' AS
WITH all_dates AS (
  SELECT CAST(tpep_pickup_datetime AS DATE) AS d FROM nyc_taxi.silver.yellow_trips
  UNION SELECT CAST(lpep_pickup_datetime AS DATE) FROM nyc_taxi.silver.green_trips
  UNION SELECT CAST(pickup_datetime AS DATE)      FROM nyc_taxi.silver.fhv_trips
  UNION SELECT CAST(pickup_datetime AS DATE)      FROM nyc_taxi.silver.fhvhv_trips
),
bounds AS (
  SELECT MIN(d) AS mn, MAX(d) AS mx FROM all_dates WHERE d IS NOT NULL
),
cal AS (
  SELECT explode(sequence((SELECT mn FROM bounds), (SELECT mx FROM bounds), INTERVAL 1 DAY)) AS full_date
)
SELECT
  CAST(date_format(full_date, 'yyyyMMdd') AS INT) AS date_key,
  full_date,
  YEAR(full_date)        AS year,
  MONTH(full_date)       AS month,
  DAY(full_date)         AS day,
  QUARTER(full_date)     AS quarter,
  WEEKOFYEAR(full_date)  AS week_of_year,
  DAYOFWEEK(full_date)   AS day_of_week,
  DATE_FORMAT(full_date, 'EEEE') AS day_name,
  CASE WHEN DAYOFWEEK(full_date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM cal;

-- COMMAND ----------

-- DBTITLE 1,Validacao
SELECT 'dim_service_type' AS dim, COUNT(*) AS linhas FROM nyc_taxi.gold.dim_service_type
UNION ALL SELECT 'dim_vendor',       COUNT(*) FROM nyc_taxi.gold.dim_vendor
UNION ALL SELECT 'dim_rate_code',    COUNT(*) FROM nyc_taxi.gold.dim_rate_code
UNION ALL SELECT 'dim_payment_type', COUNT(*) FROM nyc_taxi.gold.dim_payment_type
UNION ALL SELECT 'dim_hvfhs_license',COUNT(*) FROM nyc_taxi.gold.dim_hvfhs_license
UNION ALL SELECT 'dim_zone',         COUNT(*) FROM nyc_taxi.gold.dim_zone
UNION ALL SELECT 'dim_time',         COUNT(*) FROM nyc_taxi.gold.dim_time
UNION ALL SELECT 'dim_date',         COUNT(*) FROM nyc_taxi.gold.dim_date;
