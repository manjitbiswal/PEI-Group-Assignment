from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

CATALOG = "main"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

GOLD_TABLE = f"{GOLD}.profit_by_year_category_subcategory_customer"

GOLD_KEYS = [
    "order_year",
    "category",
    "sub_category",
    "customer_id",
    "customer_name"
]


def aggregate_profit(orders):
    return (
        orders
        .groupBy(*GOLD_KEYS)
        .agg(
            F.round(F.sum("profit"), 2)
            .cast(DecimalType(18, 2))
            .alias("total_profit")
        )
    )


def merge_gold(spark, result, affected_years):
    if not affected_years:
        return

    years = sorted(set(int(year) for year in affected_years))

    if not spark.catalog.tableExists(GOLD_TABLE):
        (
            result.write
            .format("delta")
            .mode("errorifexists")
            .saveAsTable(GOLD_TABLE)
        )
        return

    target = DeltaTable.forName(spark, GOLD_TABLE)

    condition = " AND ".join(
        [f"target.{key} <=> source.{key}" for key in GOLD_KEYS]
    )

    year_condition = (
        "target.order_year IN (" +
        ",".join(str(year) for year in years) +
        ")"
    )

    (
        target.alias("target")
        .merge(result.alias("source"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete(condition=year_condition)
        .execute()
    )


def build_profit_aggregate(spark, affected_years):
    if not affected_years:
        return

    orders = (
        spark.table(f"{SILVER}.enriched_orders")
        .filter(F.col("order_year").isin(affected_years))
    )

    result = aggregate_profit(orders)

    merge_gold(spark, result, affected_years)


def run_sql_aggregates(spark):
    queries = {
        "Profit by Year": """
            SELECT order_year, ROUND(SUM(profit), 2) AS total_profit
            FROM main.silver.enriched_orders
            GROUP BY order_year
            ORDER BY order_year
        """,

        "Profit by Year and Product Category": """
            SELECT order_year, category,
                   ROUND(SUM(profit), 2) AS total_profit
            FROM main.silver.enriched_orders
            GROUP BY order_year, category
            ORDER BY order_year, category
        """,

        "Profit by Customer": """
            SELECT customer_id, customer_name,
                   ROUND(SUM(profit), 2) AS total_profit
            FROM main.silver.enriched_orders
            GROUP BY customer_id, customer_name
            ORDER BY total_profit DESC
        """,

        "Profit by Customer and Year": """
            SELECT customer_id, customer_name, order_year,
                   ROUND(SUM(profit), 2) AS total_profit
            FROM main.silver.enriched_orders
            GROUP BY customer_id, customer_name, order_year
            ORDER BY customer_id, order_year
        """
    }

    return {
        title: spark.sql(query)
        for title, query in queries.items()
    }


def run_gold(spark, affected_years):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")

    build_profit_aggregate(spark, affected_years)

    return run_sql_aggregates(spark)