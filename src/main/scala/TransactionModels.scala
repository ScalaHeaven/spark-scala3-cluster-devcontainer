import java.sql.Timestamp

type CsvString = String | Null
type CsvInt = Int | Null
type CsvDouble = Double | Null

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
