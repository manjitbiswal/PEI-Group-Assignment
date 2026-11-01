from src.silver import (
    transform_products,
    transform_customers,
    transform_orders
)


def test_product_cleaning(products_df):

    result = transform_products(products_df)

    rows = {
        row["product_id"]: row
        for row in result.collect()
    }

    product = rows["OFF-PA-10003349"]

    assert product["category"] == "Office Supplies"
    assert product["product_name"] == "Xerox 1957"
    assert str(product["price_per_product"]) == "5.184"


def test_customer_transformation(customers_df):

    result = transform_customers(customers_df)

    row = result.filter(
        result.customer_id == "PW-19240"
    ).first()

    assert row["customer_name"] == "Pierre Wener"
    assert row["email"] == "test@gmail.com"
    assert row["country"] == "United States"


def test_order_enrichment(orders_df, customers_df, products_df):

    customers = transform_customers(customers_df)
    products = transform_products(products_df)

    result = transform_orders(
        orders_df,
        customers,
        products
    )

    assert result.count() == 2

    row = result.filter(
        result.order_id == "CA-2016-122581"
    ).first()

    assert row["customer_name"] == "Pierre Wener"
    assert row["country"] == "United States"
    assert row["category"] == "Furniture"
    assert row["sub_category"] == "Chairs"


def test_order_profit_is_rounded(orders_df, customers_df, products_df):

    customers = transform_customers(customers_df)
    products = transform_products(products_df)

    result = transform_orders(
        orders_df,
        customers,
        products
    )

    row = result.filter(
        result.order_id == "CA-2016-122581"
    ).first()

    assert str(row["profit"]) == "63.69"


def test_unmatched_customer_does_not_drop_order(
    spark, orders_df, customers_df, products_df
):

    customers = transform_customers(customers_df)

    # Remove one customer to simulate an unmatched foreign key.
    customers = customers.filter(
        customers.customer_id != "PW-19240"
    )

    products = transform_products(products_df)

    result = transform_orders(
        orders_df,
        customers,
        products
    )

    assert result.count() == 2

    row = result.filter(
        result.order_id == "CA-2016-122581"
    ).first()

    assert row["customer_name"] is None