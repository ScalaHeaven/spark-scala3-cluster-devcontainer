import org.apache.spark.sql.types.{
  DoubleType,
  IntegerType,
  StringType,
  StructField,
  StructType
}

object TransactionSchema {
  val csv: StructType = StructType(
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
}
