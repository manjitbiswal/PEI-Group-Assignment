import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("sales-pipeline-tests")
        .getOrCreate()
    )

    yield session

    session.stop()


@pytest.fixture
def products_df(spark):

    data = [
        ("FUR-CH-10002961", "Furniture", "Chairs",
         "Leather Task Chair, Black", "New York", "81.882"),

        ("OFF-PA-10003349", "Office   Supplies", "Paper",
         "Xerox   1957", "Florida", "5.184")
    ]

    columns = [
        "Product ID", "Category", "Sub-Category",
        "Product Name", "State", "Price per product"
    ]

    return spark.createDataFrame(data, columns)


@pytest.fixture
def customers_df(spark):

    data = [
        ("PW-19240", "Pierre Wener", "test@gmail.com",
         "1234567890", "Consumer", "United States",
         "Louisville", "Colorado", "80027", "West"),

        ("GH-14410", "Gary Hansen", "gary@gmail.com",
         "9876543210", "Home Office", "United States",
         "Chicago", "Illinois", "60653", "Central")
    ]

    columns = [
        "Customer ID", "Customer Name", "email", "phone",
        "Segment", "Country", "City", "State",
        "Postal Code", "Region"
    ]

    return spark.createDataFrame(data, columns)


@pytest.fixture
def orders_df(spark):

    data = [
        (1, "CA-2016-122581", "21/8/2016", "25/8/2016",
         "Standard Class", "PW-19240", "FUR-CH-10002961",
         7, 573.17, 0.3, 63.694),

        (2, "CA-2017-117485", "23/9/2017", "29/9/2017",
         "Standard Class", "GH-14410", "OFF-PA-10003349",
         4, 291.96, 0.0, 102.194)
    ]

    columns = [
        "Row ID", "Order ID", "Order Date", "Ship Date",
        "Ship Mode", "Customer ID", "Product ID",
        "Quantity", "Price", "Discount", "Profit"
    ]

    return spark.createDataFrame(data, columns)