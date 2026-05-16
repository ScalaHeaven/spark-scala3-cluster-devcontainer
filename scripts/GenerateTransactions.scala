//> using scala 3.8.3
//> using options -Yexplicit-nulls

import java.io.BufferedWriter
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object GenerateTransactions {
  private val DefaultOutputPath = "data/input/transactions.csv"
  private val DefaultRowCount = 100000
  private val Header = List(
    "transaction_id",
    "customer_id",
    "event_ts",
    "region",
    "country",
    "product_id",
    "product_category",
    "quantity",
    "unit_price",
    "discount_pct",
    "payment_method",
    "status"
  ).mkString(",")

  private val Regions = Region.values.toVector
  private val Products = Product.values.toVector
  private val PaymentMethods = PaymentMethod.values.toVector
  private val Statuses = Vector(
    TransactionStatus.Completed,
    TransactionStatus.Completed,
    TransactionStatus.Completed,
    TransactionStatus.Refunded
  )
  private val TimestampFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")

  private enum Region(val code: String, val country: String):
    case UnitedStates extends Region("NA", "US")
    case Canada extends Region("NA", "CA")
    case Germany extends Region("EMEA", "DE")
    case France extends Region("EMEA", "FR")
    case GreatBritain extends Region("EMEA", "GB")
    case Japan extends Region("APAC", "JP")
    case Singapore extends Region("APAC", "SG")
    case Brazil extends Region("LATAM", "BR")

  private enum Product(
      val id: String,
      val category: String,
      val basePrice: Double
  ):
    case HardwarePremium extends Product("sku-1001", "hardware", 129.99)
    case HardwareStandard extends Product("sku-1002", "hardware", 79.50)
    case SoftwareStandard extends Product("sku-2001", "software", 249.00)
    case SoftwareEnterprise extends Product("sku-2002", "software", 399.00)
    case ServicesPremium extends Product("sku-3001", "services", 150.00)
    case ServicesStandard extends Product("sku-3002", "services", 95.00)

  private enum PaymentMethod(val label: String):
    case Card extends PaymentMethod("card")
    case Invoice extends PaymentMethod("invoice")
    case Wire extends PaymentMethod("wire")
    case Wallet extends PaymentMethod("wallet")

  private enum TransactionStatus(val label: String):
    case Completed extends TransactionStatus("completed")
    case Refunded extends TransactionStatus("refunded")

  final case class Config(outputPath: Path, rowCount: Int)

  private enum CliCommand:
    case Run(config: Config)
    case ShowUsage

  def main(args: Array[String]): Unit =
    parseArgs(args) match {
      case Left(message) =>
        System.err.println(message)
        System.exit(1)

      case Right(CliCommand.ShowUsage) =>
        println(usage)

      case Right(CliCommand.Run(config)) =>
        writeTransactions(config)
        println(
          s"Wrote ${config.rowCount} transactions to ${config.outputPath}"
        )
    }

  private def parseArgs(args: Array[String]): Either[String, CliCommand] =
    args.toList match {
      case Nil =>
        Right(
          CliCommand.Run(Config(Paths.get(DefaultOutputPath), DefaultRowCount))
        )
      case "--help" :: Nil =>
        Right(CliCommand.ShowUsage)
      case outputPath :: Nil =>
        Right(CliCommand.Run(Config(Paths.get(outputPath), DefaultRowCount)))
      case outputPath :: rowCount :: Nil =>
        parseRowCount(rowCount).map { parsedRowCount =>
          CliCommand.Run(Config(Paths.get(outputPath), parsedRowCount))
        }
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
      region.code,
      region.country,
      product.id,
      product.category,
      quantity.toString,
      f"$unitPrice%.2f",
      f"$discountPct%.2f",
      paymentMethod.label,
      status.label
    ).mkString(",")
  }
}
