-- Section 2 — Hive (run in spark-sql)

-- a. Database
CREATE DATABASE IF NOT EXISTS ecomdb_pramodvchalla;
USE ecomdb_pramodvchalla;

-- b. External table over raw orders
CREATE TABLE orders_raw (
    order_id STRING,
    customer_id STRING,
    order_status STRING,
    order_purchase_timestamp STRING,
    order_approved_at STRING,
    order_delivered_timestamp STRING,
    order_estimated_delivery_date STRING
)
USING csv
OPTIONS (
    path 'hdfs:///user/clouduser/ecommerce/raw/orders/',
    header 'true',
    inferSchema 'false'
);

SELECT * FROM orders_raw LIMIT 5;

-- c. Managed table partitioned by year and month
CREATE TABLE orders_part (
    order_id STRING,
    customer_id STRING,
    order_status STRING,
    order_purchase_timestamp STRING,
    order_approved_at STRING,
    order_delivered_timestamp STRING,
    order_estimated_delivery_date STRING,
    order_year INT,
    order_month INT
)
USING parquet
PARTITIONED BY (order_year, order_month);

-- d. Dynamic partitioning and load
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT INTO orders_part
SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_timestamp,
    order_estimated_delivery_date,
    YEAR(TO_TIMESTAMP(order_purchase_timestamp)),
    MONTH(TO_TIMESTAMP(order_purchase_timestamp))
FROM orders_raw;

-- e. Partitions
SHOW PARTITIONS orders_part;

-- f. Orders per status
SELECT
    order_status,
    COUNT(*) AS order_count
FROM orders_part
GROUP BY order_status
ORDER BY order_count DESC;

-- g. External table over raw customers
CREATE TABLE customer (
    customer_id STRING,
    customer_zip_code_prefix STRING,
    customer_city STRING,
    customer_state STRING
)
USING csv
OPTIONS (
    path 'hdfs:///user/clouduser/ecommerce/raw/customers/',
    header 'true',
    inferSchema 'false'
);

SELECT * FROM customer LIMIT 5;

-- h. Top 10 cities by unique delivered orders
SELECT
    c.customer_city,
    COUNT(DISTINCT o.order_id) AS delivered_unique_orders
FROM customer c
JOIN orders_part o
    ON c.customer_id = o.customer_id
WHERE LOWER(o.order_status) = 'delivered'
GROUP BY c.customer_city
ORDER BY delivered_unique_orders DESC
LIMIT 10;
