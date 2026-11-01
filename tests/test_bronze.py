from src.bronze import add_metadata, read_products


def test_bronze_adds_ingestion_metadata(products_df):

    df = add_metadata(products_df)

    assert "_ingestion_timestamp" in df.columns
    assert "_source_file" in df.columns

    assert df.count() == products_df.count()


def test_products_csv_preserves_source_values(spark, tmp_path):

    file_path = tmp_path / "products.csv"

    file_path.write_text(
        'Product ID,Category,Sub-Category,Product Name,State,Price per product\n'
        'OFF-PA-10003349,"Office   Supplies",Paper,"Xerox   1957",Florida,5.184\n',
        encoding="utf-8"
    )

    df = read_products(spark, str(file_path))

    row = df.first()

    assert row["Category"] == "Office   Supplies"
    assert row["Product Name"] == "Xerox   1957"
    assert row["Price per product"] == "5.184"


def test_bronze_does_not_remove_duplicate_records(spark):

    data = [
        ("P1", "Furniture"),
        ("P1", "Furniture")
    ]

    df = spark.createDataFrame(data, ["Product ID", "Category"])

    result = add_metadata(df)

    assert result.count() == 2