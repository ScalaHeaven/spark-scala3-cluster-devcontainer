//> using scala 3.8.3
//> using options -Yexplicit-nulls
//> using dep org.apache.spark:spark-sql_2.13:3.5.1
//> using dep com.netflix.wick::wick:0.0.4
//> using exclude org.apache.spark:spark-sql_2.13

import java.sql.Timestamp
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
import org.apache.spark.sql.types.{
  DoubleType,
  StructType,
  StringType,
  StructField,
  IntegerType
}
import org.apache.spark.storage.StorageLevel

object Main {
  private type CsvString = String | Null
  private type CsvInt = Int | Null
  private type CsvDouble = Double | Null

  private val DefaultInputPath = "data/input/transactions.csv"
  private val DefaultOutputPath = "target/spark-output/transaction-summary"
  private val DefaultMaster = "local-cluster[3,1,4096]"
  private val AnalysisPartitions = "12"
  private val ExecutorMemory = "3g"
  private val DriverScalaLibraryPath =
    Option(
      classOf[scala.collection.immutable.ArraySeq[?]]
        .getProtectionDomain()
        .getCodeSource()
    )
      .map(_.getLocation().toURI().getPath())
      .getOrElse("")
  private val SparkJavaOptions = Seq(
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
  )

  private val TransactionSchema = StructType(
    Seq(
      StructField("transaction_id", StringType, nullable = false),
      StructField("customer_id", StringType, nullable = false),
      StructField("event_ts", StringType, nullable = false),
      StructField("region", StringType, nullable = false),
      StructField("country", StringType, nullable = false),
      StructField("product_id", StringType, nullable = false),
      StructField("product_category", StringType, nullable = false),
      StructField("quantity", IntegerType, nullable = false),
      StructField("unit_price", DoubleType, nullable = false),
      StructField("discount_pct", DoubleType, nullable = false),
      StructField("payment_method", StringType, nullable = false),
      StructField("status", StringType, nullable = false),
      StructField("_corrupt_record", StringType, nullable = true)
    )
  )

  final case class JobConfig(
      inputPath: String,
      outputPath: String,
      master: String
  )

  private enum CliCommand:
    case Run(config: JobConfig)
    case ShowUsage

  final case class RawTransaction(
      transaction_id: CsvString,
      customer_id: CsvString,
      event_ts: CsvString,
      region: CsvString,
      country: CsvString,
      product_id: CsvString,
      product_category: CsvString,
      quantity: CsvInt,
      unit_price: CsvDouble,
      discount_pct: CsvDouble,
      payment_method: CsvString,
      status: CsvString,
      `_corrupt_record`: CsvString
  )

  final case class EnrichedTransaction(
      transaction_id: String,
      customer_id: String,
      event_ts: String,
      region: CsvString,
      country: CsvString,
      product_id: CsvString,
      product_category: CsvString,
      quantity: Int,
      unit_price: Double,
      discount_pct: Double,
      payment_method: CsvString,
      status: CsvString,
      event_time: Timestamp,
      event_date: Timestamp,
      gross_amount: Double,
      net_amount: Double
  )

  final case class TransactionSummary(
      event_date: Timestamp,
      region: CsvString,
      country: CsvString,
      product_category: CsvString,
      status: CsvString,
      transaction_count: Long,
      unique_customers: Long,
      units_sold: Long,
      gross_revenue: Double,
      net_revenue: Double
  )

  def main(args: Array[String]): Unit =
    parseArgs(args) match {
      case Left(message) =>
        System.err.println(message)
        System.exit(1)

      case Right(CliCommand.ShowUsage) =>
        println(usage)

      case Right(CliCommand.Run(config)) =>
        val spark = createSparkSession(config)
        spark.sparkContext.setLogLevel("WARN")

        try {
          runPipeline(spark, config)
        } finally {
          spark.stop()
        }
    }

  private def parseArgs(args: Array[String]): Either[String, CliCommand] =
    args.toList match {
      case Nil =>
        Right(
          CliCommand.Run(
            JobConfig(DefaultInputPath, DefaultOutputPath, DefaultMaster)
          )
        )
      case "--help" :: Nil =>
        Right(CliCommand.ShowUsage)
      case inputPath :: outputPath :: Nil =>
        Right(CliCommand.Run(JobConfig(inputPath, outputPath, DefaultMaster)))
      case inputPath :: outputPath :: master :: Nil =>
        Right(CliCommand.Run(JobConfig(inputPath, outputPath, master)))
      case _ =>
        Left(usage)
    }

  private def createSparkSession(config: JobConfig): SparkSession = {
    val javaOptions = SparkJavaOptions.mkString(" ")
    val builder = SparkSession
      .builder()
      .appName("Large CSV Transaction Pipeline")
      .master(config.master)
      .config("spark.sql.shuffle.partitions", AnalysisPartitions)
      .config("spark.default.parallelism", AnalysisPartitions)
      .config("spark.executor.instances", "3")
      .config("spark.executor.memory", ExecutorMemory)
      .config("spark.executor.extraClassPath", DriverScalaLibraryPath)
      .config("spark.driver.extraJavaOptions", javaOptions)
      .config("spark.executor.extraJavaOptions", javaOptions)
      .config("spark.executorEnv.SPARK_SCALA_VERSION", "2.13")
      .config("spark.sql.parquet.compression.codec", "gzip")

    if (config.master.startsWith("local")) {
      builder.bindLocalDriver.getOrCreate()
    } else {
      builder.getOrCreate()
    }
  }

  extension (builder: SparkSession.Builder)
    private def bindLocalDriver: SparkSession.Builder =
      builder
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")

  private def usage: String =
    s"""Usage: sbt "run [input_csv] [output_dir] [spark_master]"
       |
       |Defaults:
       |  input_csv    $DefaultInputPath
       |  output_dir   $DefaultOutputPath
       |  spark_master $DefaultMaster
       |
       |Expected CSV header:
       |  transaction_id,customer_id,event_ts,region,country,product_id,product_category,quantity,unit_price,discount_pct,payment_method,status
       |""".stripMargin

  private def runPipeline(spark: SparkSession, config: JobConfig): Unit = {
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

  private def readTransactions(
      spark: SparkSession,
      inputPath: String
  ): DataSeq[RawTransaction] =
    DataSeq[RawTransaction](
      spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(TransactionSchema)
        .csv(inputPath)
    )

  private def transformTransactions(
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

  private def summarizeTransactions(
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
