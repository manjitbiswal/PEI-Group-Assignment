from delta.tables import DeltaTable
from pyspark.sql import functions as F

CATALOG = "main"
BRONZE = f"{CATALOG}.bronze"

RAW_PATH = "/Volumes/main/sales/raw"
STATE_PATH = "/Volumes/main/sales/_pipeline_state"

EXCEL_TRACKING_TABLE = f"{CATALOG}.bronze.excel_file_tracking"


def create_bronze_schema(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE}")

def read_products(spark, path):
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("quote", '"')
        .option("escape", '"')
        .load(path)
    )


def add_metadata(df):
    return (
        df
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )


def ingest_autoloader(
    spark,
    source_path,
    table_name,
    file_format,
    schema_path,
    checkpoint_path
):
    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.includeExistingFiles", "true")
    )

    if file_format == "csv":
        reader = (
            reader
            .option("header", "true")
            .option("quote", '"')
            .option("escape", '"')
        )

    elif file_format == "json":
        reader = reader.option("multiLine", "true")

    df = reader.load(source_path)

    df = (
        df
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    query = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(f"{BRONZE}.{table_name}")
    )

    query.awaitTermination()


def ingest_products(spark):
    ingest_autoloader(
        spark=spark,
        source_path=f"{RAW_PATH}/products/",
        table_name="products",
        file_format="csv",
        schema_path=f"{STATE_PATH}/products/schema",
        checkpoint_path=f"{STATE_PATH}/products/checkpoint"
    )


def ingest_orders(spark):
    ingest_autoloader(
        spark=spark,
        source_path=f"{RAW_PATH}/orders/",
        table_name="orders",
        file_format="json",
        schema_path=f"{STATE_PATH}/orders/schema",
        checkpoint_path=f"{STATE_PATH}/orders/checkpoint"
    )


def create_excel_tracking_table(spark):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {EXCEL_TRACKING_TABLE} (
            source_file STRING,
            processed_at TIMESTAMP
        )
        USING DELTA
    """)


def read_customers_excel(spark, file_path):
    return (
        spark.read
        .format("com.crealytics.spark.excel")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("dataAddress", "'Sheet1'!A1")
        .load(file_path)
    )


def ingest_customers(spark):
    create_excel_tracking_table(spark)

    processed_files = {
        row["source_file"]
        for row in spark.table(EXCEL_TRACKING_TABLE).select("source_file").collect()
    }

    files = [
        file.path
        for file in dbutils.fs.ls(f"{RAW_PATH}/customers/")
        if file.path.lower().endswith(".xlsx")
    ]

    for file_path in files:
        if file_path in processed_files:
            continue

        df = read_customers_excel(spark, file_path)

        source_columns = df.columns

        df = (
            df
            .withColumn("_source_file", F.lit(file_path))
            .withColumn("_ingestion_timestamp", F.current_timestamp())
            .withColumn(
                "_record_hash",
                F.sha2(
                    F.to_json(F.struct(*[F.col(c) for c in source_columns])),
                    256
                )
            )
        )

        bronze_table = f"{BRONZE}.customers"

        if not spark.catalog.tableExists(bronze_table):
            (
                df.write
                .format("delta")
                .mode("append")
                .saveAsTable(bronze_table)
            )
        else:
            target = DeltaTable.forName(spark, bronze_table)

            (
                target.alias("target")
                .merge(
                    df.alias("source"),
                    """
                    target._source_file = source._source_file
                    AND target._record_hash = source._record_hash
                    """
                )
                .whenNotMatchedInsertAll()
                .execute()
            )

        # Mark the file processed only after its data has been written.
        tracking_df = spark.createDataFrame(
            [(file_path,)],
            ["source_file"]
        ).withColumn("processed_at", F.current_timestamp())

        tracking = DeltaTable.forName(spark, EXCEL_TRACKING_TABLE)

        (
            tracking.alias("target")
            .merge(
                tracking_df.alias("source"),
                "target.source_file = source.source_file"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )


def run_bronze(spark):
    create_bronze_schema(spark)

    ingest_products(spark)
    ingest_orders(spark)
    ingest_customers(spark)