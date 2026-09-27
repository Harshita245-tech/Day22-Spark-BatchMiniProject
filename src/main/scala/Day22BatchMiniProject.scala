import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day22BatchMiniProject {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 22 - Batch Mini Project")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    println("==============================================")
    println("DAY 22 - E-COMMERCE BATCH PIPELINE")
    println("==============================================")

    // ============================================================
    // 1. Raw Transaction Data
    // ============================================================

    val transactionData = Seq(
      ("T001", "C001", "P001", "2026-09-20", 2, 75000.0),
      ("T002", "C002", "P002", "2026-09-20", 1, 35000.0),
      ("T003", "C001", "P003", "2026-09-20", 2, 5000.0),
      ("T004", "C003", "P004", "2026-09-21", 1, 28000.0),
      ("T005", "C004", "P005", "2026-09-21", 3, 3000.0),
      ("T006", "C005", "P006", "2026-09-21", 1, 22000.0),
      ("T007", "C006", "P007", "2026-09-22", 2, 1500.0),
      ("T008", "C007", "P008", "2026-09-22", 1, 12000.0),
      ("T009", "C008", "P009", "2026-09-22", 1, 45000.0),
      ("T010", "C009", "P010", "2026-09-23", 2, 18000.0),

      // Invalid records
      ("T011", "C010", "P011", "2026-09-23", 0, 5000.0),
      ("T012", "", "P012", "2026-09-23", 1, 7000.0),
      ("T013", "C011", "", "2026-09-23", 1, 8000.0),
      ("T014", "C012", "P014", "2026-09-23", -1, 6000.0),
      ("T015", "C013", "P015", "2026-09-24", 2, -5000.0)
    )

    val transactions = transactionData.toDF(
      "transaction_id",
      "customer_id",
      "product_id",
      "transaction_date",
      "quantity",
      "unit_price"
    )

    println("\n===== RAW TRANSACTIONS =====")
    transactions.show(false)

    println(s"Raw transaction count: ${transactions.count()}")

    // ============================================================
    // 2. Clean Invalid Records
    // ============================================================

    val cleanedTransactions = transactions
      .filter(col("transaction_id").isNotNull && trim(col("transaction_id")) =!= "")
      .filter(col("customer_id").isNotNull && trim(col("customer_id")) =!= "")
      .filter(col("product_id").isNotNull && trim(col("product_id")) =!= "")
      .filter(col("transaction_date").isNotNull)
      .filter(col("quantity") > 0)
      .filter(col("unit_price") > 0)

    println("\n===== CLEANED TRANSACTIONS =====")
    cleanedTransactions.show(false)

    println(s"Valid transaction count: ${cleanedTransactions.count()}")
    println(
      s"Invalid transaction count: ${transactions.count() - cleanedTransactions.count()}"
    )

    // ============================================================
    // 3. Customer Master Data
    // ============================================================

    val customerData = Seq(
      ("C001", "Aarav", "Hyderabad"),
      ("C002", "Diya", "Bangalore"),
      ("C003", "Rahul", "Chennai"),
      ("C004", "Ananya", "Mumbai"),
      ("C005", "Arjun", "Delhi"),
      ("C006", "Meera", "Pune"),
      ("C007", "Vikram", "Hyderabad"),
      ("C008", "Kiran", "Bangalore"),
      ("C009", "Priya", "Chennai")
    )

    val customers = customerData.toDF(
      "customer_id",
      "customer_name",
      "customer_city"
    )

    println("\n===== CUSTOMER DATA =====")
    customers.show(false)

    // ============================================================
    // 4. Product Master Data
    // ============================================================

    val productData = Seq(
      ("P001", "Laptop", "Electronics"),
      ("P002", "Mobile", "Electronics"),
      ("P003", "Headphones", "Accessories"),
      ("P004", "Tablet", "Electronics"),
      ("P005", "Keyboard", "Accessories"),
      ("P006", "Monitor", "Electronics"),
      ("P007", "Mouse", "Accessories"),
      ("P008", "Smart Watch", "Wearables"),
      ("P009", "Camera", "Electronics"),
      ("P010", "Printer", "Electronics")
    )

    val products = productData.toDF(
      "product_id",
      "product_name",
      "category"
    )

    println("\n===== PRODUCT DATA =====")
    products.show(false)

    // ============================================================
    // 5. Join Transactions with Customers
    // ============================================================

    val customerJoined = cleanedTransactions
      .join(customers, Seq("customer_id"), "inner")

    println("\n===== TRANSACTIONS + CUSTOMERS =====")
    customerJoined.show(false)

    // ============================================================
    // 6. Join with Product Data
    // ============================================================

    val enrichedTransactions = customerJoined
      .join(products, Seq("product_id"), "inner")

    println("\n===== ENRICHED TRANSACTIONS =====")
    enrichedTransactions.show(false)

    // ============================================================
    // 7. Calculate Revenue
    // ============================================================

    val salesWithRevenue = enrichedTransactions
      .withColumn(
        "revenue",
        round(col("quantity") * col("unit_price"), 2)
      )
      .withColumn(
        "transaction_date_parsed",
        to_date(col("transaction_date"))
      )
      .withColumn(
        "year",
        year(col("transaction_date_parsed"))
      )
      .withColumn(
        "month",
        month(col("transaction_date_parsed"))
      )
      .withColumn(
        "day",
        dayofmonth(col("transaction_date_parsed"))
      )

    println("\n===== SALES WITH REVENUE =====")
    salesWithRevenue.show(false)

    // ============================================================
    // 8. Daily Revenue Aggregation
    // ============================================================

    val dailySales = salesWithRevenue
      .groupBy(
        "year",
        "month",
        "day"
      )
      .agg(
        count("transaction_id").alias("transaction_count"),
        sum("quantity").alias("total_quantity"),
        round(sum("revenue"), 2).alias("total_revenue"),
        round(avg("revenue"), 2).alias("average_transaction_value")
      )
      .orderBy("year", "month", "day")

    println("\n===== DAILY SALES SUMMARY =====")
    dailySales.show(false)

    // ============================================================
    // 9. Category Revenue
    // ============================================================

    val categorySales = salesWithRevenue
      .groupBy("category")
      .agg(
        count("transaction_id").alias("transaction_count"),
        sum("quantity").alias("total_quantity"),
        round(sum("revenue"), 2).alias("total_revenue")
      )
      .orderBy(desc("total_revenue"))

    println("\n===== CATEGORY SALES =====")
    categorySales.show(false)

    // ============================================================
    // 10. City-wise Revenue
    // ============================================================

    val citySales = salesWithRevenue
      .groupBy("customer_city")
      .agg(
        count("transaction_id").alias("transaction_count"),
        round(sum("revenue"), 2).alias("total_revenue")
      )
      .orderBy(desc("total_revenue"))

    println("\n===== CITY-WISE SALES =====")
    citySales.show(false)

    // ============================================================
    // 11. Customer Revenue
    // ============================================================

    val customerSales = salesWithRevenue
      .groupBy("customer_id", "customer_name")
      .agg(
        count("transaction_id").alias("transaction_count"),
        round(sum("revenue"), 2).alias("total_revenue")
      )
      .orderBy(desc("total_revenue"))

    println("\n===== CUSTOMER SALES =====")
    customerSales.show(false)

    // ============================================================
    // 12. Write Partitioned Parquet Output
    // ============================================================

    println("\n===== WRITING PARTITIONED PARQUET =====")

    dailySales
      .repartition(2)
      .write
      .mode("overwrite")
      .partitionBy("year", "month", "day")
      .parquet("output/daily_sales")

    println("Daily partitioned Parquet written to:")
    println("output/daily_sales")

    // ============================================================
    // 13. Write Enriched Sales Output
    // ============================================================

    salesWithRevenue
      .drop("transaction_date_parsed")
      .repartition(2)
      .write
      .mode("overwrite")
      .partitionBy("year", "month")
      .parquet("output/enriched_sales")

    println("Enriched sales Parquet written to:")
    println("output/enriched_sales")

    // ============================================================
    // 14. Read the Partitioned Output
    // ============================================================

    println("\n===== READING PARTITIONED OUTPUT =====")

    val storedDailySales = spark.read
      .parquet("output/daily_sales")

    storedDailySales
      .orderBy("year", "month", "day")
      .show(false)

    // ============================================================
    // 15. Pipeline Statistics
    // ============================================================

    println("\n===== PIPELINE STATISTICS =====")

    println(s"Raw records       : ${transactions.count()}")
    println(s"Valid records     : ${cleanedTransactions.count()}")
    println(
      s"Invalid records   : ${transactions.count() - cleanedTransactions.count()}"
    )
    println(s"Enriched records  : ${salesWithRevenue.count()}")

    val totalRevenue = salesWithRevenue
      .agg(sum("revenue"))
      .first()
      .getDouble(0)

    println(f"Total revenue     : $totalRevenue%.2f")

    // ============================================================
    // 16. Batch Pipeline Explanation
    // ============================================================

    println("\n===== END-TO-END BATCH PIPELINE =====")

    println("1. Read raw transaction data.")
    println("2. Validate and clean invalid records.")
    println("3. Join transactions with customer master.")
    println("4. Join transactions with product master.")
    println("5. Calculate transaction revenue.")
    println("6. Extract year, month and day.")
    println("7. Aggregate daily sales.")
    println("8. Write partitioned Parquet output.")
    println("9. Read the stored output for verification.")

    println("\n===== OUTPUT LAYOUT =====")

    println("output/daily_sales/")
    println("├── year=2026/")
    println("│   └── month=9/")
    println("│       ├── day=20/")
    println("│       ├── day=21/")
    println("│       ├── day=22/")
    println("│       ├── day=23/")
    println("│       └── day=24/")
    println("└── _SUCCESS")

    println("\n==============================================")
    println("DAY 22 BATCH MINI PROJECT COMPLETED")
    println("==============================================")

    spark.stop()
  }
}
