# E-Commerce Sales & Customer Analytics — Data Engineering Pipeline

An end-to-end data engineering pipeline built on an e-commerce orders dataset using **HDFS, Hive (Spark SQL), Alteryx, Spark SQL, Kafka and Spark Structured Streaming**.

The scenario: as a Data Engineer at a fictional e-commerce platform ("RetailBridge"), ingest raw order, customer, item and payment data into HDFS, model it in Hive, prepare it in Alteryx, analyse it with Spark SQL, and process it in real time with Kafka + Spark Structured Streaming.

## Repository structure

```
.
├── alteryx/
│   ├── WF1_pramodvchalla.yxmd        # Top 20 orders by payment installments
│   └── WF2_pramodvchalla.yxmd        # Revenue summary
├── data/
│   ├── Orders.csv
│   ├── OrderItems.csv
│   ├── Customers.csv
│   ├── Payments.csv
│   └── Metadata.txt                  # Column descriptions for every table
├── docs/
│   ├── Ecommerce_Data_Engineering_Assessment_Questions.pdf
│   └── Handson_Screenshots.pdf       # Commands and outputs captured during execution
├── output/
│   └── Top20_Orders_pramodvchalla.csv
└── scripts/
    ├── hdfs_commands.sh
    └── hive_queries.sql
```

## Dataset

~89K orders across four related tables (joined on `order_id` / `customer_id`):

| Table | Columns |
|---|---|
| Orders | order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_timestamp, order_estimated_delivery_date |
| OrderItems | order_id, product_id, seller_id, price, shipping_charges |
| Customers | customer_id, customer_zip_code_prefix, customer_city, customer_state |
| Payments | order_id, payment_sequential, payment_type, payment_installments, payment_value |

See [`data/Metadata.txt`](data/Metadata.txt) for full descriptions.

## Pipeline

### 1. HDFS — storage layer
- Created `/user/clouduser/ecommerce/raw/{orders,order_items,payments,customers}` and `/processed`.
- Uploaded `orders.csv` with a **256 MB block size and replication factor 1**, verified with `hdfs fsck`.
- Uploaded order items and payments with a 128 MB block size, customers with the default.
- Checked disk usage and copied orders into `processed/`.

Commands: [`scripts/hdfs_commands.sh`](scripts/hdfs_commands.sh)

### 2. Hive — data modelling
- Database `ecomdb_pramodvchalla`.
- External table `orders_raw` over the raw orders directory.
- Managed table `orders_part` **partitioned by `order_year` and `order_month`**, loaded with dynamic partitioning (24 partitions, 2016-09 → 2018-09).
- External table `customer` over raw customers.
- Analysis: order counts per status, and the top 10 cities by delivered orders.

Queries: [`scripts/hive_queries.sql`](scripts/hive_queries.sql)

**Orders by status**

| order_status | count |
|---|---|
| delivered | 87,428 |
| shipped | 936 |
| canceled | 409 |
| processing | 273 |
| invoiced | 266 |
| approved | 2 |
| unavailable | 2 |

**Top 10 cities by delivered orders:** São Paulo (14,020), Rio de Janeiro (6,062), Belo Horizonte (2,380), Brasília (1,740), Curitiba (1,405), Porto Alegre (1,290), Campinas (1,283), Salvador (1,115), Guarulhos (1,045), São Bernardo do Campo (829).

### 3. Alteryx — data preparation
- **WF1** (`alteryx/WF1_pramodvchalla.yxmd`): Join Orders + Payments → filter out null/empty `order_status` → trim `payment_type` → default null `payment_installments` to 1 → sort descending → take the top 20 → [`output/Top20_Orders_pramodvchalla.csv`](output/Top20_Orders_pramodvchalla.csv).
- **WF2** (`alteryx/WF2_pramodvchalla.yxmd`): Join Orders + OrderItems + Payments → `Order_value = price + shipping_charges` → summarise.

| Sum_Order_value | Count | Avg payment_installments |
|---|---|---|
| 34,403,072.06 | 89,316 | 2.97 |

> The workflows reference absolute Windows paths for their input and output files. Re-point the Input/Output Data tools to the `data/` folder before running them on another machine.

### 4. Spark SQL — analytics
- **city_payment_summary**: total orders, total payment value and average installments per `customer_city`, written as Parquet to `/user/clouduser/ecommerce/processed/city_payment_summary`.
- **delivery_analysis**: average and maximum delivery days per `customer_state`, ranked with a window function (`delay_rank`) and flagged `High Delay` (> 15 days) or `On-Track`, written as Parquet to `/user/clouduser/ecommerce/processed/delivery_analysis`.

### 5. Kafka + Spark Structured Streaming — real-time processing
- Created Kafka topic `order-stream` and published `orders.csv` row by row with the console producer.
- Spark Structured Streaming reads from the topic, applies a **1-minute tumbling window** on the event timestamp and counts orders per `order_status`.
- Output goes to the console (`outputMode = update`, 30-second trigger) and to HDFS as Parquet.

## Tech stack
Hadoop HDFS · Apache Hive / Spark SQL · Apache Spark (PySpark) · Apache Kafka · Spark Structured Streaming · Alteryx Designer · WSL (Ubuntu)

## Evidence
Every step's command and output is captured in [`docs/Handson_Screenshots.pdf`](docs/Handson_Screenshots.pdf).

## Author
**Pramod V Challa**
