import com.netflix.wick.{*, given}
import com.netflix.wick.functions.{count, countDistinct, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{
  col,
  lit,
  round,
  to_date,
  try_to_timestamp
}
import org.apache.spark.storage.StorageLevel

object TransactionPipeline {
  def run(spark: SparkSession, config: JobConfig): Unit = {
    val rawTransactions = readTransactions(spark, config.inputPath)
    val cleanedTransactions = transformTransactions(rawTransactions)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val rawCount = rawTransactions.dataFrame.count()
    val cleanedCount = cleanedTransactions.dataFrame.count()
    val summary = summarizeTransactions(cleanedTransactions)

    summary.write
      .mode("overwrite")
      .option("compression", "gzip")
      .partitionBy("event_date")
      .parquet(config.outputPath)

    println(s"Read $rawCount CSV rows from ${config.inputPath}")
    println(s"Wrote $cleanedCount valid rows into ${config.outputPath}")

    cleanedTransactions.dataFrame.unpersist()
  }

  def readTransactions(
      spark: SparkSession,
      inputPath: String
  ): DataSeq[RawTransaction] =
    DataSeq[RawTransaction](
      spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(TransactionSchema.csv)
        .csv(inputPath)
    )

  def transformTransactions(
      transactions: DataSeq[RawTransaction]
  ): DataSeq[EnrichedTransaction] = {
    val validTransactions = transactions.filter { row =>
      row.`_corrupt_record`.isNull &&
      row.transaction_id.isNotNull &&
      row.customer_id.isNotNull &&
      row.event_ts.isNotNull &&
      row.quantity.orElse(0) > 0 &&
      row.unit_price.orElse(-1.0) >= 0.0 &&
      row.discount_pct.orElse(-1.0) >= 0.0 &&
      row.discount_pct.orElse(2.0) <= 1.0
    }

    DataSeq[EnrichedTransaction](
      validTransactions.dataFrame
        .withColumn(
          "event_time",
          try_to_timestamp(col("event_ts"), lit("yyyy-MM-dd'T'HH:mm:ss"))
        )
        .filter(col("event_time").isNotNull)
        .withColumn("event_date", to_date(col("event_time")))
        .withColumn(
          "gross_amount",
          col("quantity") * col("unit_price")
        )
        .withColumn(
          "net_amount",
          round(
            col("gross_amount") *
              (lit(1.0) - col("discount_pct")),
            2
          )
        )
        .drop("_corrupt_record")
    )
  }

  def summarizeTransactions(
      transactions: DataSeq[EnrichedTransaction]
  ): DataFrame = {
    val aggregated = transactions
      .groupBy { transaction =>
        (
          event_date = transaction.event_date,
          region = transaction.region,
          country = transaction.country,
          product_category = transaction.product_category,
          status = transaction.status
        )
      }
      .agg { transaction =>
        (
          transaction_count = count(transaction.transaction_id),
          unique_customers = countDistinct(transaction.customer_id),
          units_sold = sum(transaction.quantity),
          gross_revenue = sum(transaction.gross_amount),
          net_revenue = sum(transaction.net_amount)
        )
      }

    val rounded = aggregated.dataFrame.select(
      col("event_date"),
      col("region"),
      col("country"),
      col("product_category"),
      col("status"),
      col("transaction_count"),
      col("unique_customers"),
      col("units_sold"),
      round(col("gross_revenue"), 2).as("gross_revenue"),
      round(col("net_revenue"), 2).as("net_revenue")
    )

    DataSeq[TransactionSummary](rounded).orderBy { summary =>
      (
        summary.event_date,
        summary.region,
        summary.country,
        summary.product_category,
        summary.status
      )
    }.dataFrame
  }
}
