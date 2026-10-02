#!/usr/bin/env bash
# Section 1 — HDFS
# Run from the folder that holds the CSV files.

# Start HDFS and check the daemons
start-dfs.sh
jps

# a. Create the directory layout
hdfs dfs -mkdir -p /user/clouduser/ecommerce/raw/orders
hdfs dfs -mkdir -p /user/clouduser/ecommerce/raw/order_items
hdfs dfs -mkdir -p /user/clouduser/ecommerce/raw/payments
hdfs dfs -mkdir -p /user/clouduser/ecommerce/raw/customers
hdfs dfs -mkdir -p /user/clouduser/ecommerce/processed
hdfs dfs -ls -R /user/clouduser/ecommerce

# b. Orders: 256 MB block size, replication factor 1
hdfs dfs -D dfs.blocksize=268435456 -D dfs.replication=1 -put orders.csv /user/clouduser/ecommerce/raw/orders/
hdfs dfs -ls /user/clouduser/ecommerce/raw/orders/
hdfs fsck /user/clouduser/ecommerce/raw/orders/orders.csv -files -blocks -locations

# c. Order items and payments: 128 MB block size
hdfs dfs -D dfs.blocksize=134217728 -put order_items.csv /user/clouduser/ecommerce/raw/order_items/
hdfs dfs -D dfs.blocksize=134217728 -put Payments.csv /user/clouduser/ecommerce/raw/payments/

# d. Customers: default block size
hdfs dfs -put customers.csv /user/clouduser/ecommerce/raw/customers/
hdfs dfs -ls /user/clouduser/ecommerce/raw/customers/

# e. Disk usage
hdfs dfs -du -h /user/clouduser/ecommerce

# f. Copy (not move) orders to processed
hdfs dfs -cp /user/clouduser/ecommerce/raw/orders/orders.csv /user/clouduser/ecommerce/processed/
hdfs dfs -ls /user/clouduser/ecommerce/raw/orders/
hdfs dfs -ls /user/clouduser/ecommerce/processed/
