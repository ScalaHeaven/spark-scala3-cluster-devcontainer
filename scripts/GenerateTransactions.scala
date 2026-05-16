//> using scala 3.8.3
//> using options -Yexplicit-nulls

import java.io.BufferedWriter
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object GenerateTransactions {
  private val DefaultOutputPath = "data/input/transactions.csv"
  private val DefaultRowCount = 100000
  private val Header =
    "transaction_id,customer_id,event_ts,region,country,product_id,product_category,quantity,unit_price,discount_pct,payment_method,status"

  private val Regions = Vector(
    Region("NA", "US"),
    Region("NA", "CA"),
    Region("EMEA", "DE"),
    Region("EMEA", "FR"),
    Region("EMEA", "GB"),
    Region("APAC", "JP"),
    Region("APAC", "SG"),
    Region("LATAM", "BR")
  )

  private val Products = Vector(
    Product("sku-1001", "hardware", 129.99),
    Product("sku-1002", "hardware", 79.50),
    Product("sku-2001", "software", 249.00),
    Product("sku-2002", "software", 399.00),
    Product("sku-3001", "services", 150.00),
    Product("sku-3002", "services", 95.00)
  )

  private val PaymentMethods = Vector("card", "invoice", "wire", "wallet")
  private val Statuses =
    Vector("completed", "completed", "completed", "refunded")
  private val TimestampFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")

  final case class Region(region: String, country: String)

  final case class Product(id: String, category: String, basePrice: Double)

  final case class Config(outputPath: Path, rowCount: Int)

  def main(args: Array[String]): Unit =
    parseArgs(args) match {
      case Left(message) =>
        System.err.println(message)
        System.exit(1)

      case Right(config) =>
        writeTransactions(config)
        println(
          s"Wrote ${config.rowCount} transactions to ${config.outputPath}"
        )
    }

  private def parseArgs(args: Array[String]): Either[String, Config] =
    args.toList match {
      case Nil =>
        Right(Config(Paths.get(DefaultOutputPath), DefaultRowCount))
      case "--help" :: Nil =>
        Left(usage)
      case outputPath :: Nil =>
        Right(Config(Paths.get(outputPath), DefaultRowCount))
      case outputPath :: rowCount :: Nil =>
        parseRowCount(rowCount).map(Config(Paths.get(outputPath), _))
      case _ =>
        Left(usage)
    }

  private def parseRowCount(value: String): Either[String, Int] =
    try {
      val rowCount = value.toInt
      if (rowCount > 0) {
        Right(rowCount)
      } else {
        Left("Row count must be greater than zero.")
      }
    } catch {
      case _: NumberFormatException =>
        Left(s"Invalid row count: $value")
    }

  private def usage: String =
    s"""Usage: scala-cli scripts/GenerateTransactions.scala -- [output_csv] [row_count]
       |
       |Defaults:
       |  output_csv $DefaultOutputPath
       |  row_count  $DefaultRowCount
       |""".stripMargin

  private def writeTransactions(config: Config): Unit = {
    Option(config.outputPath.getParent).foreach(Files.createDirectories(_))

    val writer = Files.newBufferedWriter(
      config.outputPath,
      StandardCharsets.UTF_8
    )

    try {
      writeRows(writer, config.rowCount)
    } finally {
      writer.close()
    }
  }

  private def writeRows(writer: BufferedWriter, rowCount: Int): Unit = {
    writer.write(Header)
    writer.newLine()

    (1 to rowCount).foreach { rowNumber =>
      writer.write(transactionRow(rowNumber))
      writer.newLine()
    }
  }

  private def transactionRow(rowNumber: Int): String = {
    val region = Regions((rowNumber - 1) % Regions.length)
    val product = Products((rowNumber * 7) % Products.length)
    val timestamp = LocalDateTime
      .of(2026, 5, 1, 0, 0, 0)
      .plusMinutes((rowNumber - 1).toLong * 11L)
    val quantity = (rowNumber % 5) + 1
    val priceAdjustment = ((rowNumber % 19) - 9).toDouble
    val unitPrice = product.basePrice + priceAdjustment
    val discountPct = (rowNumber % 6).toDouble / 100.0
    val paymentMethod = PaymentMethods(rowNumber % PaymentMethods.length)
    val status = Statuses(rowNumber % Statuses.length)

    Vector(
      f"tx-$rowNumber%06d",
      f"cust-${(rowNumber % 5000) + 1}%05d",
      timestamp.format(TimestampFormatter),
      region.region,
      region.country,
      product.id,
      product.category,
      quantity.toString,
      f"$unitPrice%.2f",
      f"$discountPct%.2f",
      paymentMethod,
      status
    ).mkString(",")
  }
}
