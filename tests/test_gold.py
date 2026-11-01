from decimal import Decimal

from src.gold import aggregate_profit


def test_profit_aggregation(spark):

    data = [
        (2016, "Furniture", "Chairs", "C1", "Pierre", 63.69),
        (2016, "Furniture", "Chairs", "C1", "Pierre", 20.31),
        (2017, "Technology", "Accessories", "C2", "Gary", 102.19)
    ]

    columns = [
        "order_year",
        "category",
        "sub_category",
        "customer_id",
        "customer_name",
        "profit"
    ]

    df = spark.createDataFrame(data, columns)

    result = aggregate_profit(df)

    assert result.count() == 2

    row = result.filter(
        result.customer_id == "C1"
    ).first()

    assert row["total_profit"] == Decimal("84.00")


def test_gold_aggregation_groups_by_year(spark):

    data = [
        (2016, "Furniture", "Chairs", "C1", "Pierre", 50.00),
        (2017, "Furniture", "Chairs", "C1", "Pierre", 70.00)
    ]

    columns = [
        "order_year",
        "category",
        "sub_category",
        "customer_id",
        "customer_name",
        "profit"
    ]

    df = spark.createDataFrame(data, columns)

    result = aggregate_profit(df)

    assert result.count() == 2


def test_gold_aggregation_preserves_category_and_customer_dimensions(spark):

    data = [
        (2016, "Furniture", "Chairs", "C1", "Pierre", 50.00),
        (2016, "Technology", "Accessories", "C1", "Pierre", 70.00)
    ]

    columns = [
        "order_year",
        "category",
        "sub_category",
        "customer_id",
        "customer_name",
        "profit"
    ]

    df = spark.createDataFrame(data, columns)

    result = aggregate_profit(df)

    assert result.count() == 2