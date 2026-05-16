final case class JobConfig(
    inputPath: String,
    outputPath: String,
    master: String
)

enum CliCommand:
  case Run(config: JobConfig)
  case ShowUsage

object Cli {
  val DefaultInputPath = "data/input/transactions.csv"
  val DefaultOutputPath = "target/spark-output/transaction-summary"
  val DefaultMaster = "local-cluster[3,1,200]"

  def parseArgs(args: Array[String]): Either[String, CliCommand] =
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

  def usage: String =
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
}
