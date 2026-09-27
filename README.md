# 🚀 Day 22 - Apache Spark Batch Mini Project

## 📌 Overview

This project demonstrates an **end-to-end batch data processing pipeline using Apache Spark and Scala**.

The scenario represents a real-world **E-commerce Daily Sales Pipeline** where raw transaction data is processed, cleaned, enriched with customer and product information, aggregated, and stored as partitioned Parquet output.

The project covers the complete batch processing workflow:

- Reading raw transaction data
- Reading customer data
- Reading product data
- Cleaning invalid transaction records
- Validating transaction data
- Joining transaction, customer, and product data
- Calculating revenue
- Aggregating sales metrics
- Partitioning output data
- Writing processed data as Parquet
- Building a complete batch ETL pipeline

---

## 🎯 Objectives

- Understand end-to-end batch processing using Spark
- Read raw transaction data
- Identify and remove invalid records
- Validate transaction amounts and required fields
- Join transaction data with customer data
- Join transaction data with product data
- Calculate transaction revenue
- Aggregate daily sales and revenue
- Write processed data in Parquet format
- Partition output for efficient querying
- Understand a practical data engineering pipeline

---

## 🛠️ Technologies Used

- **Apache Spark 3.5.3**
- **Scala 2.12.18**
- **Spark SQL**
- **SBT**
- **Parquet**
- **Ubuntu/Linux**
- **Git & GitHub**

---

## 📂 Project Structure

    day22-spark/
    │
    ├── src/
    │   └── main/
    │       └── scala/
    │           └── Day22BatchMiniProject.scala
    │
    ├── data/
    │   ├── transactions.csv
    │   ├── customers.csv
    │   └── products.csv
    │
    ├── project/
    │   └── build.properties
    │
    ├── build.sbt
    ├── .gitignore
    └── README.md

Generated Spark output is intentionally not committed to GitHub.

---

# 🛒 Scenario - E-commerce Daily Sales Pipeline

The project represents an e-commerce company processing its daily sales transactions.

The pipeline receives three datasets:

### 🧾 Transactions

The transaction dataset contains:

- Transaction ID
- Customer ID
- Product ID
- Transaction Date
- Quantity
- Transaction Amount

Example:

| Transaction ID | Customer ID | Product ID | Date | Quantity | Amount |
|---|---|---|---|---:|---:|
| T001 | C001 | P001 | 2026-09-22 | 2 | 50000 |
| T002 | C002 | P002 | 2026-09-22 | 1 | 30000 |
| T003 | C003 | P003 | 2026-09-22 | 3 | 15000 |
| T004 | C004 | P001 | 2026-09-22 | 1 | 25000 |
| T005 | C005 | P004 | 2026-09-22 | 2 | 10000 |

---

## 👥 Customer Data

The customer dataset contains:

- Customer ID
- Customer Name
- City
- State

Example:

| Customer ID | Customer Name | City | State |
|---|---|---|---|
| C001 | Rahul | Hyderabad | Telangana |
| C002 | Priya | Bangalore | Karnataka |
| C003 | Arjun | Chennai | Tamil Nadu |
| C004 | Sneha | Hyderabad | Telangana |
| C005 | Kiran | Mumbai | Maharashtra |

---

## 📦 Product Data

The product dataset contains:

- Product ID
- Product Name
- Category
- Price

Example:

| Product ID | Product Name | Category | Price |
|---|---|---|---:|
| P001 | Laptop | Electronics | 25000 |
| P002 | Mobile | Electronics | 30000 |
| P003 | Headphones | Accessories | 5000 |
| P004 | Keyboard | Accessories | 5000 |

---

# 🔄 End-to-End Batch Pipeline

The complete pipeline follows:

    Raw Transactions
          ↓
    Data Validation
          ↓
    Clean Invalid Records
          ↓
    Join Customer Data
          ↓
    Join Product Data
          ↓
    Calculate Revenue
          ↓
    Aggregate Sales
          ↓
    Partition Data
          ↓
    Write Parquet
          ↓
    Final Analytics Dataset

---

# 📥 Step 1 - Read Raw Transactions

The first step is to read the raw transaction data.

    val transactions =
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/transactions.csv")

The transaction DataFrame contains the raw sales records received from the e-commerce system.

---

# 🧹 Step 2 - Clean Invalid Records

Raw transaction data may contain invalid records.

Examples of invalid records include:

- Missing transaction ID
- Missing customer ID
- Missing product ID
- Invalid quantity
- Zero quantity
- Negative quantity
- Invalid transaction amount
- Null values

The pipeline filters invalid records before performing further processing.

Example:

    val cleanTransactions =
      transactions
        .filter(col("transaction_id").isNotNull)
        .filter(col("customer_id").isNotNull)
        .filter(col("product_id").isNotNull)
        .filter(col("quantity") > 0)
        .filter(col("amount") > 0)

This ensures that only valid transactions continue through the pipeline.

---

# 👥 Step 3 - Read Customer Data

Customer information is loaded into a Spark DataFrame.

    val customers =
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/customers.csv")

Customer information is used to enrich the transaction records.

---

# 📦 Step 4 - Read Product Data

Product information is also loaded.

    val products =
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/products.csv")

The product dataset provides:

- Product name
- Product category
- Product price

---

# 🔗 Step 5 - Join Customer Data

Clean transactions are joined with customer information using `customer_id`.

    val customerJoined =
      cleanTransactions
        .join(customers, Seq("customer_id"), "inner")

The resulting dataset contains both transaction and customer information.

---

# 🔗 Step 6 - Join Product Data

The customer-enriched transactions are then joined with product information using `product_id`.

    val enrichedTransactions =
      customerJoined
        .join(products, Seq("product_id"), "inner")

The resulting dataset contains:

- Transaction information
- Customer information
- Product information
- Product category
- Product price

---

# 💰 Step 7 - Calculate Revenue

Revenue is calculated from the transaction data.

    val salesWithRevenue =
      enrichedTransactions
        .withColumn(
          "revenue",
          col("quantity") * col("price")
        )

This creates a new `revenue` column.

The basic calculation is:

    Revenue = Quantity × Product Price

---

# 📊 Step 8 - Aggregate Sales

The pipeline calculates sales metrics.

Example:

    val dailySales =
      salesWithRevenue
        .groupBy("transaction_date")
        .agg(
          count("transaction_id").alias("transaction_count"),
          sum("quantity").alias("total_quantity"),
          sum("revenue").alias("total_revenue"),
          avg("revenue").alias("average_revenue")
        )
        .orderBy("transaction_date")

The output contains:

- Transaction count
- Total quantity
- Total revenue
- Average revenue

---

# 🏷️ Category-Wise Revenue

The project can also calculate revenue by product category.

    val categorySales =
      salesWithRevenue
        .groupBy("category")
        .agg(
          count("transaction_id").alias("transaction_count"),
          sum("revenue").alias("total_revenue")
        )
        .orderBy("total_revenue")

This helps identify sales performance across different product categories.

---

# 🌆 City-Wise Revenue

Customer information can also be used to analyze revenue by city.

    val citySales =
      salesWithRevenue
        .groupBy("city")
        .agg(
          count("transaction_id").alias("transaction_count"),
          sum("revenue").alias("total_revenue")
        )
        .orderBy("total_revenue")

This provides geographical sales insights.

---

# 🗂️ Step 9 - Partitioned Output

The processed sales data is written as partitioned Parquet.

Example:

    salesWithRevenue
      .write
      .mode("overwrite")
      .partitionBy("transaction_date")
      .parquet("output/daily_sales")

This creates a directory structure based on the transaction date.

Example:

    output/daily_sales/
    │
    ├── transaction_date=2026-09-20/
    ├── transaction_date=2026-09-21/
    ├── transaction_date=2026-09-22/
    └── _SUCCESS

Partitioning allows Spark to efficiently access data for specific dates.

---

# 🗃️ Why Use Parquet?

Parquet is used as the final output format because it provides:

- Columnar storage
- Compression
- Schema information
- Efficient analytical queries
- Column pruning
- Better Spark performance

Parquet is commonly used for processed data in data lake environments.

---

# 🔍 Data Quality Checks

The pipeline performs basic data quality validation before processing.

Important checks include:

- Transaction ID should not be null
- Customer ID should not be null
- Product ID should not be null
- Quantity should be greater than zero
- Amount should be greater than zero

Invalid records are removed before joins and aggregations.

---

# 📈 Final Output

The final processed dataset contains enriched sales information such as:

| Transaction ID | Customer | Product | Category | City | Quantity | Price | Revenue |
|---|---|---|---|---|---:|---:|---:|
| T001 | Rahul | Laptop | Electronics | Hyderabad | 2 | 25000 | 50000 |
| T002 | Priya | Mobile | Electronics | Bangalore | 1 | 30000 | 30000 |
| T003 | Arjun | Headphones | Accessories | Chennai | 3 | 5000 | 15000 |
| T004 | Sneha | Laptop | Electronics | Hyderabad | 1 | 25000 | 25000 |
| T005 | Kiran | Keyboard | Accessories | Mumbai | 2 | 5000 | 10000 |

---

# 🧠 Key Concepts Learned

### 1. Batch Processing

Batch processing processes a collection of data at scheduled intervals rather than processing every record immediately.

### 2. Data Cleaning

Invalid records should be removed before performing joins and aggregations.

### 3. Data Enrichment

Joining transactions with customer and product datasets adds useful business information.

### 4. Aggregation

Spark can calculate business metrics such as transaction count, quantity, and revenue.

### 5. Partitioning

Partitioning organizes output data into separate directories based on selected columns.

### 6. Parquet

Parquet provides efficient columnar storage for analytical workloads.

### 7. End-to-End ETL

The complete pipeline follows:

    Extract
       ↓
    Transform
       ↓
    Load

---

# 🔄 Complete ETL Architecture

    ┌─────────────────────┐
    │  Raw Transactions   │
    └──────────┬──────────┘
               ↓
    ┌─────────────────────┐
    │   Data Validation   │
    │  & Cleaning         │
    └──────────┬──────────┘
               ↓
    ┌─────────────────────┐
    │ Customer Data        │
    └──────────┬──────────┘
               │
               ↓
    ┌─────────────────────┐
    │ Customer Join        │
    └──────────┬──────────┘
               ↓
    ┌─────────────────────┐
    │ Product Data         │
    └──────────┬──────────┘
               │
               ↓
    ┌─────────────────────┐
    │ Product Join         │
    └──────────┬──────────┘
               ↓
    ┌─────────────────────┐
    │ Revenue Calculation  │
    └──────────┬──────────┘
               ↓
    ┌─────────────────────┐
    │ Sales Aggregation    │
    └──────────┬──────────┘
               ↓
    ┌─────────────────────┐
    │ Partitioned Parquet  │
    └─────────────────────┘

---

# 🌍 Real-World Applications

This batch pipeline pattern is commonly used in:

- 🛒 E-commerce platforms
- 🏦 Banking systems
- 💳 Payment processing
- 📦 Supply chain systems
- 🚚 Logistics platforms
- 🏨 Hotel booking systems
- ✈️ Travel platforms
- 📊 Business intelligence systems
- 📈 Daily sales reporting

---

# ▶️ How to Run

### 1. Clone the repository

    git clone https://github.com/Harshita245-tech/Day22-Spark-Batch-MiniProject.git

### 2. Move into the project

    cd Day22-Spark-Batch-MiniProject

### 3. Compile

    sbt compile

### 4. Run

    sbt run

---

# 📌 Main File

    src/main/scala/Day22BatchMiniProject.scala

This file contains the complete end-to-end batch processing pipeline.

---

# 📚 Day 22 Learning Summary

The main workflow demonstrated in this project is:

    Raw Transaction Data
             ↓
       Data Cleaning
             ↓
      Customer Join
             ↓
       Product Join
             ↓
      Revenue Calculation
             ↓
       Sales Aggregation
             ↓
       Data Partitioning
             ↓
      Parquet Output

This exercise demonstrates how Apache Spark can be used to build a complete batch data engineering pipeline for an e-commerce daily sales scenario.

---

## ✅ Project Status

**Day 22 - Batch Mini Project: COMPLETED 🎉**
