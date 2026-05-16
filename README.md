# Scala 3 Spark Devcontainer

Ready-to-open Scala 3 workspace for developing and running an Apache Spark CSV
pipeline in VS Code Dev Containers.

This repository gives you:

- Scala `3.8.3`, sbt `1.12.11`, and Apache Spark SQL `3.5.1`
- Wick `0.0.4` for typed Spark DataFrame transformations
- a VS Code devcontainer with JDK 21, Scala CLI, sbt, Metals, Codex, and Metals MCP
- JDK source archives linked into the devcontainer JDK for Java standard library
  navigation from Metals
- a deterministic 100,000-row transaction CSV sample
- a production Dockerfile that builds and runs an assembly JAR

The example app reads a fixed-schema transaction CSV, filters invalid rows,
adds event date and revenue columns, aggregates reporting metrics, and writes
partitioned Parquet output. By default it runs against a local Spark standalone
cluster with one master and three worker JVMs, so development behaves like a
small cluster without any external services.

## Quick Start

Open this folder in VS Code, then run **Dev Containers: Reopen in Container**.
The container opens at `/workspaces/spark-scala3-cluster-devcontainer`.

On startup, the devcontainer repairs common workspace ownership issues, syncs
the minimal `/opt/spark` home to the Spark version pinned by the build, configures
Git/Codex integration when host files are mounted, and starts Metals MCP.

Inside the container:

```bash
sbt -Dsbt.batch=true compile
sbt -Dsbt.batch=true run
```

Regenerate the sample CSV before running if you want a fresh copy:

```bash
scala-cli scripts/GenerateTransactions.scala -- data/input/transactions.csv 100000
```

Run with explicit input and output paths:

```bash
sbt -Dsbt.batch=true "run data/input/transactions.csv target/spark-output/transaction-summary"
```

Show CLI usage:

```bash
sbt -Dsbt.batch=true "run --help"
```

## Runtime Defaults

When no arguments are supplied, the app uses:

```text
input_csv    data/input/transactions.csv
output_dir   target/spark-output/transaction-summary
spark_master local-cluster[3,1,200]
```

`local-cluster[3,1,200]` starts one local standalone master and three worker
JVMs. Each worker has one core and 200 MiB of worker memory. Executors are
configured with `spark.executor.memory=200m`, and Spark's reserved-memory floor
is disabled with `spark.testing.reservedMemory=0` so the low-memory development
cluster can start. The driver JVM uses `-Xmx4g` for `sbt run`, VS Code debug
launches, and the production Docker image.

To target another Spark master, pass it as the third argument:

```bash
sbt -Dsbt.batch=true "run /data/landing/transactions.csv /data/curated/transaction-summary spark://spark-master:7077"
```

For external clusters, make sure the input and output paths are reachable from
both the driver and executors.

## CSV Contract

The reader uses an explicit Spark `StructType`; it does not infer CSV types.
Input files must include this header:

```csv
transaction_id,customer_id,event_ts,region,country,product_id,product_category,quantity,unit_price,discount_pct,payment_method,status
```

Fields:

- `transaction_id`: unique transaction identifier
- `customer_id`: customer identifier
- `event_ts`: timestamp in `yyyy-MM-dd'T'HH:mm:ss` format
- `region`: business region, such as `NA`, `EMEA`, or `APAC`
- `country`: country code used for reporting
- `product_id`: product SKU
- `product_category`: reporting category
- `quantity`: positive integer quantity
- `unit_price`: non-negative item price
- `discount_pct`: decimal discount from `0.0` through `1.0`
- `payment_method`: payment channel label
- `status`: transaction status, such as `completed` or `refunded`

Rows are excluded before aggregation when they are malformed, have missing
required identifiers or timestamps, have invalid timestamps, use non-positive
quantities, use negative prices, or have discounts outside `0.0` through `1.0`.

## Pipeline Output

The job writes gzip-compressed Parquet files partitioned by `event_date`.
Rows are grouped by:

- `event_date`
- `region`
- `country`
- `product_category`
- `status`

Metrics:

- `transaction_count`
- `unique_customers`
- `units_sold`
- `gross_revenue`
- `net_revenue`

The default output directory is overwritten on each run:

```text
target/spark-output/transaction-summary
```

## Build And Validation

Run sbt commands serially. Starting multiple sbt processes at the same time can
hit the sbt boot socket lock and fail with `ServerAlreadyBootingException`.

```bash
sbt -Dsbt.batch=true compile
sbt -Dsbt.batch=true run
sbt -Dsbt.batch=true assembly
sbt -Dsbt.batch=true scalafmtCheckAll
```

Format Scala sources with:

```bash
sbt -Dsbt.batch=true scalafmtAll
```

Build and run the production image:

```bash
docker build -t spark-scala3-cluster-devcontainer .
docker run --rm spark-scala3-cluster-devcontainer
```

## Project Layout

- `build.sbt`: pins Scala, Spark SQL, and Wick; enables SemanticDB; configures
  Spark Java module options; sets the `sbt run` heap; and builds `app.jar` with
  `sbt-assembly`.
- `src/main/scala/Main.scala`: application entry point.
- `src/main/scala/Cli.scala`: argument parsing and runtime defaults.
- `src/main/scala/SparkSessionFactory.scala`: Spark session construction,
  local-cluster driver binding, executor memory, Java module options, and
  Scala library classpath handling for executor RPC serialization.
- `src/main/scala/TransactionSchema.scala`: explicit Spark CSV schema.
- `src/main/scala/TransactionModels.scala`: Wick row models and nullable CSV
  boundary types.
- `src/main/scala/TransactionPipeline.scala`: CSV read, validation,
  enrichment, aggregation, ordering, and Parquet write.
- `scripts/GenerateTransactions.scala`: deterministic sample data generator.
- `data/input/transactions.csv`: checked-in 100,000-row sample CSV.
- `.devcontainer/Dockerfile`: development image with JDK 21, Scala tools, JDK
  sources for Metals navigation, and a minimal Spark home.
- `.devcontainer/post-start.sh`: idempotent startup repair and tool setup.
- `.vscode/launch.json`: Metals/Scala debug launch config for `Main`.
- `Dockerfile`: production multi-stage build that generates sample data, builds
  the assembly JAR, and runs it with the minimal Spark home.

## Spark And Scala Notes

Spark `3.5.1` is consumed through the explicit `spark-sql_2.13` artifact because
Spark publishes its Scala APIs for Scala 2.13. Application sources compile with
Scala `3.8.3`, while `SPARK_SCALA_VERSION` remains `2.13` for local-cluster
executor startup.

Local-cluster executors receive the driver's active Scala library first on their
classpath so Spark RPC serialization uses a matching Scala standard library on
both sides.
