from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

CATALOG = "main"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"


def transform_products(df):
    return df.select(
        F.trim(F.col("Product ID")).alias("product_id"),
        F.regexp_replace(
            F.translate(F.col("Category"), "\u00a0", " "),
            r"\s+", " "
        ).alias("category"),
        F.regexp_replace(
            F.translate(F.col("Sub-Category"), "\u00a0", " "),
            r"\s+", " "
        ).alias("sub_category"),
        F.regexp_replace(
            F.regexp_replace(
                F.translate(F.col("Product Name"), "\u00a0", " "),
                r"\s+", " "
            ),
            "\u00c2",
            ""
        ).alias("product_name"),
        F.regexp_replace(
            F.translate(F.col("State"), "\u00a0", " "),
            r"\s+", " "
        ).alias("state"),
        F.col("Price per product").cast(
            DecimalType(18, 3)
        ).alias("price_per_product")
    )


def transform_customers(df):
    address_col = (
        F.col("address")
        if "address" in df.columns
        else F.lit(None).cast("string")
    )

    return df.select(
        F.trim(F.col("Customer ID")).alias("customer_id"),
        F.regexp_replace(
            F.translate(F.col("Customer Name"), "\u00a0", " "),
            r"\s+", " "
        ).alias("customer_name"),
        F.lower(F.trim(F.col("email"))).alias("email"),
        F.trim(F.col("phone")).alias("phone"),
        F.regexp_replace(
            F.translate(address_col, "\u00a0", " "),
            r"\s+", " "
        ).alias("address"),
        F.trim(F.col("Segment")).alias("segment"),
        F.trim(F.col("Country")).alias("country"),
        F.trim(F.col("City")).alias("city"),
        F.trim(F.col("State")).alias("state"),
        F.trim(F.col("Postal Code")).alias("postal_code"),
        F.trim(F.col("Region")).alias("region")
    )


def transform_orders(orders, customers, products):
    orders = orders.select(
        F.col("Row ID").cast("int").alias("row_id"),
        F.col("Order ID").alias("order_id"),
        F.to_date(F.col("Order Date"), "d/M/yyyy").alias("order_date"),
        F.to_date(F.col("Ship Date"), "d/M/yyyy").alias("ship_date"),
        F.col("Ship Mode").alias("ship_mode"),
        F.trim(F.col("Customer ID")).alias("customer_id"),
        F.trim(F.col("Product ID")).alias("product_id"),
        F.col("Quantity").cast("int").alias("quantity"),
        F.col("Price").cast(DecimalType(18, 2)).alias("price"),
        F.col("Discount").cast(DecimalType(10, 4)).alias("discount"),
        F.round(
            F.col("Profit").cast(DecimalType(18, 4)), 2
        ).cast(DecimalType(18, 2)).alias("profit")
    )

    return (
        orders
        .join(customers, on="customer_id", how="left")
        .join(products, on="product_id", how="left")
        .select(
            "row_id", "order_id", "order_date", "ship_date",
            "ship_mode", "customer_id", "customer_name", "country",
            "product_id", "category", "sub_category", "product_name",
            "quantity", "price", "discount", "profit"
        )
        .withColumn("order_year", F.year("order_date"))
    )


def validate_unique_key(df, key):
    duplicates = (
        df.groupBy(key)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicates:
        raise ValueError(f"Duplicate records found for key: {key}")


def merge_silver(spark, df, table_name, key):
    validate_unique_key(df, key)

    if not spark.catalog.tableExists(table_name):
        (
            df.write
            .format("delta")
            .mode("errorifexists")
            .saveAsTable(table_name)
        )
        return

    target = DeltaTable.forName(spark, table_name)

    (
        target.alias("target")
        .merge(
            df.alias("source"),
            f"target.{key} = source.{key}"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def build_products(spark):
    df = spark.table(f"{BRONZE}.products")
    result = transform_products(df)
    merge_silver(spark, result, f"{SILVER}.enriched_products", "product_id")


def build_customers(spark):
    df = spark.table(f"{BRONZE}.customers")
    result = transform_customers(df)
    merge_silver(spark, result, f"{SILVER}.enriched_customers", "customer_id")


def build_enriched_orders(spark):
    orders = spark.table(f"{BRONZE}.orders")
    customers = transform_customers(spark.table(f"{BRONZE}.customers"))
    products = transform_products(spark.table(f"{BRONZE}.products"))

    result = transform_orders(orders, customers, products)

    merge_silver(
        spark,
        result,
        f"{SILVER}.enriched_orders",
        "row_id"
    )


def run_silver(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")

    build_products(spark)
    build_customers(spark)
    build_enriched_orders(spark)