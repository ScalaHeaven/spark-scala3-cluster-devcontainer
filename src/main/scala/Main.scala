//> using scala 3.8.3
//> using options -Yexplicit-nulls
//> using dep org.apache.spark:spark-sql_2.13:4.1.1
//> using dep com.netflix.wick::wick:0.0.4
//> using exclude org.apache.spark:spark-sql_2.13

import com.netflix.wick.{*, given}
import java.sql.Timestamp
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.functions.col as sparkCol
import org.apache.spark.sql.functions.count as sparkCount
import org.apache.spark.sql.functions.countDistinct as sparkCountDistinct
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.round as sparkRound
import org.apache.spark.sql.functions.sum as sparkSum
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.try_to_timestamp
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

object Main {
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

  final case class RawTransaction(
      transaction_id: String | Null,
      customer_id: String | Null,
      event_ts: String | Null,
      region: String | Null,
      country: String | Null,
      product_id: String | Null,
      product_category: String | Null,
      quantity: Int | Null,
      unit_price: Double | Null,
      discount_pct: Double | Null,
      payment_method: String | Null,
      status: String | Null,
      `_corrupt_record`: String | Null
  )

  final case class EnrichedTransaction(
      transaction_id: String,
      customer_id: String,
      event_ts: String,
      region: String | Null,
      country: String | Null,
      product_id: String | Null,
      product_category: String | Null,
      quantity: Int,
      unit_price: Double,
      discount_pct: Double,
      payment_method: String | Null,
      status: String | Null,
      event_time: Timestamp,
      event_date: Timestamp,
      gross_amount: Double,
      net_amount: Double
  )

  final case class TransactionSummary(
      event_date: Timestamp,
      region: String | Null,
      country: String | Null,
      product_category: String | Null,
      status: String | Null,
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

      case Right(config) =>
        val sparkBuilder = SparkSession
          .builder()
          .appName("Large CSV Transaction Pipeline")
          .master(config.master)
          .config("spark.sql.shuffle.partitions", AnalysisPartitions)
          .config("spark.default.parallelism", AnalysisPartitions)
          .config("spark.executor.instances", "3")
          .config("spark.executor.memory", ExecutorMemory)
          .config("spark.executor.extraClassPath", DriverScalaLibraryPath)
          .config(
            "spark.driver.extraJavaOptions",
            SparkJavaOptions.mkString(" ")
          )
          .config(
            "spark.executor.extraJavaOptions",
            SparkJavaOptions.mkString(" ")
          )
          .config("spark.executorEnv.SPARK_SCALA_VERSION", "2.13")
          .config("spark.sql.parquet.compression.codec", "gzip")

        val configuredBuilder =
          if (config.master.startsWith("local")) {
            sparkBuilder
              .config("spark.driver.host", "127.0.0.1")
              .config("spark.driver.bindAddress", "127.0.0.1")
          } else {
            sparkBuilder
          }

        val spark = configuredBuilder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        try {
          runPipeline(spark, config)
        } finally {
          spark.stop()
        }
    }

  private def parseArgs(args: Array[String]): Either[String, JobConfig] =
    args.toList match {
      case Nil =>
        Right(JobConfig(DefaultInputPath, DefaultOutputPath, DefaultMaster))
      case "--help" :: Nil =>
        Left(usage)
      case inputPath :: outputPath :: Nil =>
        Right(JobConfig(inputPath, outputPath, DefaultMaster))
      case inputPath :: outputPath :: master :: Nil =>
        Right(JobConfig(inputPath, outputPath, master))
      case _ =>
        Left(usage)
    }

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

  private def toSparkColumn(expr: Expr[?]): Column = {
    org.apache.spark.sql.Spark4ColumnCompat.fromCatalystExpression(
      expr.underlying
    )
  }

  private def wickFilter[T](
      dataSeq: DataSeq[T]
  )(condition: DataSeq.Ref[T] => Expr[Boolean]): DataSeq[T] =
    DataSeq[T](
      dataSeq.dataFrame.filter(
        toSparkColumn(condition(DataSeq.Ref[T](None)))
      )
    )

  private def wickColumns[T](
      dataSeq: DataSeq[T]
  )(columns: DataSeq.Ref[T] => Seq[(String, Expr[?])]): Seq[Column] =
    columns(DataSeq.Ref[T](None)).map { case (name, expr) =>
      toSparkColumn(expr).as(name)
    }

  private def wickAggregateColumns[T](
      dataSeq: DataSeq[T]
  )(columns: DataSeq.Ref[T] => Seq[(String, Column)]): Seq[Column] =
    columns(DataSeq.Ref[T](None)).map { case (name, column) =>
      column.as(name)
    }

  private def wickOrderBy[T](
      dataSeq: DataSeq[T]
  )(columns: DataSeq.Ref[T] => Seq[Expr[?]]): DataSeq[T] =
    DataSeq[T](
      dataSeq.dataFrame.sort(
        columns(DataSeq.Ref[T](None)).map(toSparkColumn)*
      )
    )

  private def transformTransactions(
      transactions: DataSeq[RawTransaction]
  ): DataSeq[EnrichedTransaction] = {
    val validTransactions = wickFilter(transactions) { row =>
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
          try_to_timestamp(sparkCol("event_ts"), lit("yyyy-MM-dd'T'HH:mm:ss"))
        )
        .filter(sparkCol("event_time").isNotNull)
        .withColumn("event_date", to_date(sparkCol("event_time")))
        .withColumn(
          "gross_amount",
          sparkCol("quantity") * sparkCol("unit_price")
        )
        .withColumn(
          "net_amount",
          sparkRound(
            sparkCol("gross_amount") *
              (lit(1.0) - sparkCol("discount_pct")),
            2
          )
        )
        .drop("_corrupt_record")
    )
  }

  private def summarizeTransactions(
      transactions: DataSeq[EnrichedTransaction]
  ): DataFrame = {
    val groupColumns = wickColumns(transactions)(row =>
      Seq(
        "event_date" -> row.event_date,
        "region" -> row.region,
        "country" -> row.country,
        "product_category" -> row.product_category,
        "status" -> row.status
      )
    )

    val aggregateColumns = wickAggregateColumns(transactions)(row =>
      Seq(
        "transaction_count" -> sparkCount(toSparkColumn(row.transaction_id)),
        "unique_customers" -> sparkCountDistinct(
          toSparkColumn(row.customer_id)
        ),
        "units_sold" -> sparkSum(toSparkColumn(row.quantity)),
        "gross_revenue" -> sparkSum(toSparkColumn(row.gross_amount)),
        "net_revenue" -> sparkSum(toSparkColumn(row.net_amount))
      )
    )

    val aggregated = transactions.dataFrame
      .groupBy(groupColumns*)
      .agg(aggregateColumns.head, aggregateColumns.tail*)

    val rounded = aggregated.select(
      sparkCol("event_date"),
      sparkCol("region"),
      sparkCol("country"),
      sparkCol("product_category"),
      sparkCol("status"),
      sparkCol("transaction_count"),
      sparkCol("unique_customers"),
      sparkCol("units_sold"),
      sparkRound(sparkCol("gross_revenue"), 2).as("gross_revenue"),
      sparkRound(sparkCol("net_revenue"), 2).as("net_revenue")
    )

    wickOrderBy(DataSeq[TransactionSummary](rounded))(row =>
      Seq(
        row.event_date,
        row.region,
        row.country,
        row.product_category,
        row.status
      )
    ).dataFrame
  }
}
